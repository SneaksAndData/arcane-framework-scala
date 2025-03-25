package com.sneaksanddata.arcane.framework
package services.lakehouse

import models.{ArcaneSchema, DataRow}
import services.lakehouse.base.{CatalogWriter, IcebergCatalogSettings, S3CatalogFileIO}

import org.apache.iceberg.aws.s3.S3FileIOProperties
import org.apache.iceberg.catalog.{Catalog, SessionCatalog, TableIdentifier}
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.parquet.GenericParquetWriter
import org.apache.iceberg.parquet.Parquet
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList
import org.apache.iceberg.rest.{HTTPClient, RESTCatalog, RESTSessionCatalog}
import org.apache.iceberg.{CatalogProperties, CatalogUtil, PartitionSpec, Schema, Table}
import zio.{Schedule, Task, ZIO, ZLayer}
import logging.ZIOLogAnnotations.*

import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

/**
 * Converts an Arcane schema to an Iceberg schema.
 */
given Conversion[ArcaneSchema, Schema] with
  def apply(schema: ArcaneSchema): Schema = SchemaConversions.toIcebergSchema(schema)
  
// https://www.tabular.io/blog/java-api-part-3/
class IcebergS3CatalogWriter(namespace: String,
                              warehouse: String,
                              catalogUri: String,
                              additionalProperties: Map[String, String],
                              s3CatalogFileIO: S3CatalogFileIO,
                              locationOverride: Option[String],
                              catalog: Catalog
                        ) extends CatalogWriter[RESTCatalog, Table, Schema]:

  private def createTable(name: String, schema: Schema): Task[Table] =
    val tableId = TableIdentifier.of(namespace, name)
    ZIO.attemptBlocking(
      locationOverride match
        case Some(newLocation) => catalog.createTable(tableId, schema, PartitionSpec.unpartitioned(), newLocation + "/" + name, Map().asJava)
        case None => catalog.createTable(tableId, schema, PartitionSpec.unpartitioned())
    )

  private def rowToRecord(row: DataRow, schema: Schema): GenericRecord =
    val record = GenericRecord.create(schema)
    val rowMap = row.map { cell => cell.name -> cell.value }.toMap
    record.copy(rowMap.asJava)

  private def appendData(data: Iterable[DataRow], schema: Schema, isTargetEmpty: Boolean)(implicit tbl: Table): Task[Table] = ZIO.attemptBlocking {
      val appendTran = tbl.newTransaction()
      // create iceberg records
      val records = data.map(r => rowToRecord(r, schema)).foldLeft(ImmutableList.builder[GenericRecord]) {
        (builder, record) => builder.add(record)
      }.build()
      val file = tbl.io.newOutputFile(s"${tbl.location()}/${UUID.randomUUID.toString}")
      val dataWriter = Parquet.writeData(file)
          .schema(tbl.schema())
          .createWriterFunc(GenericParquetWriter.buildWriter)
          .overwrite()
          .withSpec(PartitionSpec.unpartitioned())
          .build[GenericRecord]()

      Try(for (record <- records.asScala) { dataWriter.write(record) }) match {
        case Success(_) =>
          dataWriter.close()

          // only use fast append for the case when table is empty
          if isTargetEmpty then appendTran.newFastAppend().appendFile(dataWriter.toDataFile).commit()
          else appendTran.newAppend().appendFile(dataWriter.toDataFile).commit()

          appendTran.commitTransaction()
          tbl
        case Failure(ex) =>
          dataWriter.close()
          throw ex
      }
    }
  
  override def write(data: Iterable[DataRow], name: String, schema: Schema): Task[Table] =
    for table <- createTable(name, schema)
        _ <- zlog("Created a staging table %s, waiting for it to be created", name)
        _ <- ZIO.sleep(zio.Duration.fromSeconds(1)).repeatUntil(_ => catalog.tableExists(TableIdentifier.of(namespace, name)))
        _ <- zlog("Staging table %s created, appending data", name)
        updatedTable <- appendData(data, schema, false)(table)
        _ <- zlog("Staging table %s ready for merge", name)
     yield updatedTable

  override def delete(tableName: String): Task[Boolean] =
    val tableId = TableIdentifier.of(namespace, tableName)
    ZIO.attemptBlocking(catalog.dropTable(tableId))

  def append(data: Iterable[DataRow], name: String, schema: Schema): Task[Table] =
    val tableId = TableIdentifier.of(namespace, name)
    for table <- ZIO.attemptBlocking(catalog.loadTable(tableId))
      updatedTable <- appendData(data, schema, false)(table)
    yield updatedTable

object IcebergS3CatalogWriter:

  type Environment = IcebergCatalogSettings
    & Catalog

  /**
   * Factory method to create IcebergS3CatalogWriter
   *
   * @param icebergSettings Iceberg settings
   * @return The initialized IcebergS3CatalogWriter instance
   */
  def apply(icebergSettings: IcebergCatalogSettings, catalog: Catalog): IcebergS3CatalogWriter =
    new IcebergS3CatalogWriter(
      icebergSettings.namespace,
      icebergSettings.warehouse,
      icebergSettings.catalogUri,
      icebergSettings.additionalProperties,
      icebergSettings.s3CatalogFileIO,
      icebergSettings.stagingLocation,
      catalog,
    )

  /**
   * The ZLayer that creates the LazyOutputDataProcessor.
   */
  val layer: ZLayer[Environment, Throwable, IcebergS3CatalogWriter] =
    ZLayer {
      for
        settings <- ZIO.service[IcebergCatalogSettings]
        catalog <- ZIO.service[Catalog]
      yield IcebergS3CatalogWriter(settings, catalog)
    }

  extension (s: IcebergCatalogSettings) def toCatalogProperties: Map[String, String] =
    Map (
    CatalogProperties.WAREHOUSE_LOCATION -> s.warehouse,
    CatalogProperties.URI -> s.catalogUri,
    CatalogProperties.CATALOG_IMPL -> "org.apache.iceberg.rest.RESTCatalog",
    CatalogProperties.FILE_IO_IMPL -> s.s3CatalogFileIO.implClass,
    S3FileIOProperties.ENDPOINT -> s.s3CatalogFileIO.endpoint,
    S3FileIOProperties.PATH_STYLE_ACCESS -> s.s3CatalogFileIO.pathStyleEnabled,
    S3FileIOProperties.ACCESS_KEY_ID -> s.s3CatalogFileIO.accessKeyId,
    S3FileIOProperties.SECRET_ACCESS_KEY -> s.s3CatalogFileIO.secretAccessKey,
  ) ++ s.additionalProperties

