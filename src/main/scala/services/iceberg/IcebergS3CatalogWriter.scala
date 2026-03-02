package com.sneaksanddata.arcane.framework
package services.iceberg

import logging.ZIOLogAnnotations.*
import models.schemas.{ArcaneSchema, DataRow}
import models.settings.IcebergStagingSettings
import services.iceberg.base.{CatalogEntityManager, CatalogWriter, StagingEntityManager}

import org.apache.iceberg.aws.s3.{S3FileIO, S3FileIOProperties}
import org.apache.iceberg.catalog.SessionCatalog.SessionContext
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.parquet.GenericParquetWriter
import org.apache.iceberg.parquet.Parquet
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList
import org.apache.iceberg.rest.auth.OAuth2Properties
import org.apache.iceberg.rest.{HTTPClient, RESTCatalog, RESTSessionCatalog}
import org.apache.iceberg.*
import zio.*
import zio.logging.LogAnnotation

import java.time.Instant
import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions

/** Converts an Arcane schema to an Iceberg schema.
  */
given Conversion[ArcaneSchema, Schema] with
  def apply(schema: ArcaneSchema): Schema = SchemaConversions.toIcebergSchema(schema)

// https://www.tabular.io/blog/java-api-part-3/
class IcebergS3CatalogWriter(entityManager: CatalogEntityManager, writerSettings: IcebergStagingSettings)
    extends CatalogWriter[RESTCatalog, Table, Schema]:

  private val maxRowsPerFile = writerSettings.maxRowsPerFile.getOrElse(10000)

  private def rowToRecord(row: DataRow, schema: Schema): GenericRecord =
    val record = GenericRecord.create(schema)
    val rowMap = row.map { cell => cell.name -> cell.value }.toMap
    record.copy(rowMap.asJava)

  private def chunkToFile(chunk: Iterable[DataRow], schema: Schema, tbl: Table): Task[DataFile] = for
    records <- ZIO.attempt(
      chunk
        .foldLeft(ImmutableList.builder[GenericRecord]) { (builder, record) =>
          builder.add(rowToRecord(record, schema))
        }
        .build()
    )
    file <- ZIO.attemptBlocking(tbl.io.newOutputFile(tbl.locationProvider().newDataLocation(UUID.randomUUID.toString)))
    writer <- ZIO.acquireReleaseWith(
      ZIO.attempt(
        Parquet
          .writeData(file)
          .schema(tbl.schema())
          .createWriterFunc(GenericParquetWriter.create)
          .overwrite()
          .withSpec(PartitionSpec.unpartitioned())
          .build[GenericRecord]()
      )
    )(dataWriter => ZIO.attemptBlockingIO(dataWriter.close()).orDie) { dataWriter =>
      ZIO.attemptBlockingIO {
        dataWriter.write(records)
        dataWriter
      }
    }
  yield writer.toDataFile

  private def appendData(
      data: Iterable[DataRow],
      schema: Schema,
      isTargetEmpty: Boolean,
      tbl: Table,
      logAnnotations: Seq[(LogAnnotation[String], String)]
  ): Task[Table] = for
    _ <- zlog(
      "Preparing fast append of %s rows into table %s, max rows per file %s",
      logAnnotations,
      data.size.toString,
      tbl.name(),
      maxRowsPerFile.toString
    )
    chunks     <- ZIO.succeed(data.grouped(maxRowsPerFile))
    appendTran <- ZIO.attemptBlocking(tbl.newTransaction())
    files      <- ZIO.collectAllPar(chunks.map(chunk => chunkToFile(chunk, schema, tbl)).toArray)
    _          <- zlog("Created %s files to append for table %s", logAnnotations, files.length.toString, tbl.name())
    appendFilesOp <-
      if isTargetEmpty then ZIO.attempt(appendTran.newFastAppend()) else ZIO.attempt(appendTran.newAppend())
    _ <- zlog("Committing data files into table %s", logAnnotations, tbl.name())
    filesToCommit <- ZIO.foldLeft(files)(appendFilesOp) { case (agg, dataFile) =>
      ZIO.attempt(agg.appendFile(dataFile))
    }
    _ <- ZIO.attemptBlocking(filesToCommit.commit())
    _ <- ZIO.attemptBlocking(appendTran.commitTransaction())
    _ <- zlog("Fast append transaction successfully applied to table %s", logAnnotations, tbl.name())
  yield tbl

  override def write(
      data: Iterable[DataRow],
      name: String,
      schema: Schema,
      logAnnotations: Seq[(LogAnnotation[String], String)]
  ): Task[Table] =
    for
      _       <- entityManager.createTable(name, schema, false)
      _       <- zlog("Created a staging table %s, waiting for commit", logAnnotations, name)
      catalog <- entityManager.catalogFactory.getCatalog
      _ <- ZIO
        .sleep(zio.Duration.fromSeconds(1))
        .repeatUntil(_ =>
          catalog
            .tableExists(entityManager.catalogFactory.getSessionContext, TableIdentifier.of(writerSettings.namespace, name))
        )
      _ <- zlog("Staging table %s created, appending data", logAnnotations, name)
      table <- ZIO.attemptBlocking(
        catalog.loadTable(entityManager.catalogFactory.getSessionContext, TableIdentifier.of(writerSettings.namespace, name))
      )
      updatedTable <- appendData(data, schema, false, table, logAnnotations)
      _            <- zlog("Staging table %s ready for merge", logAnnotations, name)
    yield updatedTable

  def append(
      data: Iterable[DataRow],
      name: String,
      schema: Schema,
      logAnnotations: Seq[(LogAnnotation[String], String)]
  ): Task[Table] = for
    tableId      <- ZIO.succeed(TableIdentifier.of(writerSettings.namespace, name))
    catalog      <- entityManager.catalogFactory.getCatalog
    table        <- ZIO.attemptBlocking(catalog.loadTable(entityManager.catalogFactory.getSessionContext, tableId))
    updatedTable <- appendData(data, schema, false, table, logAnnotations)
  yield updatedTable

object IcebergS3CatalogWriter:

  /** Factory method to create IcebergS3CatalogWriter
    *
    * @param icebergSettings
    *   Iceberg settings
    * @return
    *   The initialized IcebergS3CatalogWriter instance
    */
  def apply(entityManager: CatalogEntityManager, writerSettings: IcebergStagingSettings): IcebergS3CatalogWriter =
    new IcebergS3CatalogWriter(entityManager, writerSettings)
    
  def layer = ZLayer {
    for
      stagingEntityManager <- ZIO.service[StagingEntityManager]
      settings <- ZIO.service[IcebergStagingSettings]
    yield IcebergS3CatalogWriter(stagingEntityManager, settings) 
  }  
