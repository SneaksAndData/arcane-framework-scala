package com.sneaksanddata.arcane.framework
package services.lakehouse

import models.{ArcaneSchema, ArcaneType, DataCell, DataRow}
import services.lakehouse.base.{CatalogWriter, IcebergCatalogSettings, IcebergDataRowConverter}

import org.apache.iceberg.aws.s3.{S3FileIO, S3FileIOProperties}
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.parquet.GenericParquetWriter
import org.apache.iceberg.parquet.Parquet
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList
import org.apache.iceberg.rest.{HTTPClient, RESTCatalog, RESTSessionCatalog}
import org.apache.iceberg.{CatalogProperties, CatalogUtil, DataFiles, PartitionSpec, Schema, Table}
import zio.{Reloadable, Schedule, Task, ZIO, ZLayer}
import logging.ZIOLogAnnotations.*

import org.apache.iceberg.catalog.SessionCatalog.SessionContext
import org.apache.iceberg.rest.auth.OAuth2Properties
import zio.*

import java.time.{Instant, OffsetDateTime}
import java.util.UUID
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}
import scala.collection.concurrent.TrieMap

/**
 * Converts an Arcane schema to an Iceberg schema.
 */
given Conversion[ArcaneSchema, Schema] with
  def apply(schema: ArcaneSchema): Schema = SchemaConversions.toIcebergSchema(schema)
  
// https://www.tabular.io/blog/java-api-part-3/
class IcebergS3CatalogWriter(icebergCatalogSettings: IcebergCatalogSettings, icebergTypeConverter: IcebergDataRowConverter)
  extends CatalogWriter[RESTCatalog, Table, Schema]:

  private val catalogProperties: Map[String, String] =
    Map(
      CatalogProperties.WAREHOUSE_LOCATION -> icebergCatalogSettings.warehouse,
      CatalogProperties.URI -> icebergCatalogSettings.catalogUri,
      CatalogProperties.FILE_IO_IMPL -> icebergCatalogSettings.s3CatalogFileIO.implClass,
      S3FileIOProperties.ENDPOINT -> icebergCatalogSettings.s3CatalogFileIO.endpoint,
      S3FileIOProperties.PATH_STYLE_ACCESS -> icebergCatalogSettings.s3CatalogFileIO.pathStyleEnabled,
      S3FileIOProperties.ACCESS_KEY_ID -> icebergCatalogSettings.s3CatalogFileIO.accessKeyId,
      S3FileIOProperties.SECRET_ACCESS_KEY -> icebergCatalogSettings.s3CatalogFileIO.secretAccessKey,
      "rest-metrics-reporting-enabled" -> "false",
      "view-endpoints-supported" -> "false",
    ) ++ icebergCatalogSettings.additionalProperties


  private val maxCatalogLifetime = zio.Duration.fromSeconds(10 * 60).toSeconds // limit catalog instance lifetime to 10 minutes
  private val catalogs: TrieMap[String, (RESTSessionCatalog, Long)] = TrieMap()
  private def getSessionContext =
    SessionContext(java.util.UUID.randomUUID().toString, null, icebergCatalogSettings.additionalProperties.filter(c => c._1 == OAuth2Properties.CREDENTIAL || c._1 == OAuth2Properties.TOKEN).asJava, Map().asJava);

  private def newCatalog = for
    result <- ZIO.succeed(new RESTSessionCatalog(
      config => HTTPClient.builder(config).uri(config.get(CatalogProperties.URI)).build(),
      (_, _) =>
        val baseIO = new S3FileIO()
        baseIO.initialize(catalogProperties.asJava)
        baseIO
    ))
    name <- ZIO.succeed(java.util.UUID.randomUUID().toString)
    _ <- zlog("Creating new Iceberg RESTSessionCatalog instance with id %s", name)
    _ <- ZIO.attemptBlocking(result.initialize(name, (catalogProperties ++ Map("header.X-Arcane-Runner-Identifier" -> name)).asJava))
    _ <- ZIO.attempt(catalogs.addOne((name, (result, Instant.now.getEpochSecond))))
  yield result


  /**
   * Rest Catalog object
   */
  private def getCatalog: ZIO[Any, Throwable, RESTSessionCatalog] = for
    catalogInfo <- ZIO.attempt(catalogs.find {
      case (_, (_, duration)) => Instant.now.getEpochSecond - duration < maxCatalogLifetime
    })
    selected <- catalogInfo match {
      case Some((_, (catalog, _))) => ZIO.succeed(catalog)
      case None => ZIO.attempt(catalogs.foreach {
        case (_, (catalog, _)) => catalog.close()
      }).map(_ => catalogs.clear()).flatMap(_ => newCatalog)
    }
  yield selected

  private def createTable(name: String, schema: Schema): Task[Table] = for
    tableId <- ZIO.succeed(TableIdentifier.of(icebergCatalogSettings.namespace, name))
    catalog <- getCatalog
    tableRef <- ZIO.attemptBlocking(
      icebergCatalogSettings.stagingLocation match
        case Some(newLocation) => catalog.buildTable(getSessionContext, tableId, schema).withLocation(newLocation + "/" + name).withPartitionSpec(PartitionSpec.unpartitioned()).create()
        case None => catalog.buildTable(getSessionContext, tableId, schema).withPartitionSpec(PartitionSpec.unpartitioned()).create()
    )
  yield tableRef

  private def rowToRecord(row: DataRow, schema: Schema): GenericRecord =
    val record = GenericRecord.create(schema)
    val rowMap = icebergTypeConverter.convert(row)
    record.copy(rowMap.asJava)

  private def appendData(data: Iterable[DataRow], schema: Schema, isTargetEmpty: Boolean, tbl: Table): Task[Table] = for
    _ <- zlog("Preparing fast append of %s rows into table %s", data.size.toString, tbl.name())
    appendTran <- ZIO.attemptBlocking(tbl.newTransaction())
    records <- ZIO.attempt(data.map(r => rowToRecord(r, schema)).foldLeft(ImmutableList.builder[GenericRecord]) {
      (builder, record) => builder.add(record)
    }.build())
    file <- ZIO.attemptBlocking(tbl.io.newOutputFile(tbl.locationProvider().newDataLocation(UUID.randomUUID.toString)))
    writer <- ZIO.acquireReleaseWith(ZIO.attempt(Parquet.writeData(file)
      .schema(tbl.schema())
      .createWriterFunc(GenericParquetWriter.buildWriter)
      .overwrite()
      .withSpec(PartitionSpec.unpartitioned())
      .build[GenericRecord]()))(dataWriter => ZIO.attemptBlockingIO(dataWriter.close()).orDie) { dataWriter =>
      ZIO.attemptBlockingIO{
        dataWriter.write(records)
        dataWriter
      }
    }
    _ <- zlog("Committing fast append into table %s", tbl.name())
    _ <- if isTargetEmpty then ZIO.attemptBlocking(appendTran.newFastAppend().appendFile(writer.toDataFile).commit()) else ZIO.attemptBlocking(appendTran.newAppend().appendFile(writer.toDataFile).commit())
    _ <- ZIO.attemptBlocking(appendTran.commitTransaction())
  yield tbl
  
  override def write(data: Iterable[DataRow], name: String, schema: Schema): Task[Table] =
    for _ <- createTable(name, schema)
        _ <- zlog("Created a staging table %s, waiting for it to be created", name)
        catalog <- getCatalog
        _ <- ZIO.sleep(zio.Duration.fromSeconds(1)).repeatUntil(_ => catalog.tableExists(getSessionContext, TableIdentifier.of(icebergCatalogSettings.namespace, name)))
        _ <- zlog("Staging table %s created, appending data", name)
        table <- ZIO.attemptBlocking(catalog.loadTable(getSessionContext, TableIdentifier.of(icebergCatalogSettings.namespace, name)))
        updatedTable <- appendData(data, schema, false, table)
        _ <- zlog("Staging table %s ready for merge", name)
     yield updatedTable

  override def delete(tableName: String): Task[Boolean] = for
    tableId <- ZIO.succeed(TableIdentifier.of(icebergCatalogSettings.namespace, tableName))
    catalog <- getCatalog
    result <- ZIO.attemptBlocking(catalog.dropTable(getSessionContext, tableId))
  yield result


  def append(data: Iterable[DataRow], name: String, schema: Schema): Task[Table] = for
    tableId <- ZIO.succeed(TableIdentifier.of(icebergCatalogSettings.namespace, name))
    catalog <- getCatalog
    table <- ZIO.attemptBlocking(catalog.loadTable(getSessionContext, tableId))
    updatedTable <- appendData(data, schema, false, table)
  yield updatedTable

object IcebergS3CatalogWriter:

  /**
   * The environment required to create an instance of IcebergS3CatalogWriter.
   */
  type Environment = IcebergCatalogSettings
    & IcebergDataRowConverter

  /**
   * Factory method to create IcebergS3CatalogWriter
   *
   * @param icebergSettings Iceberg settings
   * @return The initialized IcebergS3CatalogWriter instance
   */
  def apply(icebergSettings: IcebergCatalogSettings, icebergDataRowConverter: IcebergDataRowConverter): IcebergS3CatalogWriter =
    new IcebergS3CatalogWriter(icebergSettings, icebergDataRowConverter)

  /**
   * The ZLayer that creates the LazyOutputDataProcessor.
   */
  val layer: ZLayer[Environment, Throwable, IcebergS3CatalogWriter] =
    ZLayer {
      for
        settings <- ZIO.service[IcebergCatalogSettings]
        icebergDataRowConverter <- ZIO.service[IcebergDataRowConverter]
      yield IcebergS3CatalogWriter(settings, icebergDataRowConverter)
    }
