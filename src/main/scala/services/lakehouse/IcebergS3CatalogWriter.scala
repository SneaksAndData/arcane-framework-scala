package com.sneaksanddata.arcane.framework
package services.lakehouse

import models.{ArcaneSchema, DataRow}
import services.lakehouse.base.IcebergCatalogSettings

import org.apache.iceberg.aws.s3.S3FileIOProperties
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.parquet.GenericParquetWriter
import org.apache.iceberg.parquet.Parquet
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList
import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{CatalogProperties, PartitionSpec, Schema, Table}
import zio.{ZIO, ZLayer}

import java.util.UUID
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

/**
 * Converts an Arcane schema to an Iceberg schema.
 */
given Conversion[ArcaneSchema, Schema] with
  def apply(schema: ArcaneSchema): Schema = SchemaConversions.toIcebergSchema(schema)
  
// https://www.tabular.io/blog/java-api-part-3/
class IcebergS3CatalogWriter(
                              namespace: String,
                              warehouse: String,
                              catalogUri: String,
                              additionalProperties: Map[String, String],
                              s3CatalogFileIO: S3CatalogFileIO,
                              locationOverride: Option[String] = None,
                        ) extends CatalogWriter[RESTCatalog, Table, Schema]:

  private def createTable(name: String, schema: Schema): Future[Table] =
    val tableId = TableIdentifier.of(namespace, name)
    // TODO: add support for partition spec
    Future({
      locationOverride match
        case Some(newLocation) => catalog.createTable(tableId, schema, PartitionSpec.unpartitioned(), newLocation + "/" + name, Map().asJava)
        case None => catalog.createTable(tableId, schema, PartitionSpec.unpartitioned())
    })

  private def rowToRecord(row: DataRow, schema: Schema)(implicit tbl: Table): GenericRecord =
    val record = GenericRecord.create(schema)
    val rowMap = row.map { cell => cell.name -> cell.value }.toMap
    record.copy(rowMap.asJava)

  private def appendData(data: Iterable[DataRow], schema: Schema, isTargetEmpty: Boolean)(implicit tbl: Table): Future[Table] = Future {
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
  
  override def write(data: Iterable[DataRow], name: String, schema: Schema): Future[Table] =
    createTable(name, schema).flatMap(appendData(data, schema, true))

  override implicit val catalog: RESTCatalog = new RESTCatalog()
  override implicit val catalogProperties: Map[String, String] = Map(
    CatalogProperties.WAREHOUSE_LOCATION -> warehouse,
    CatalogProperties.URI -> catalogUri,
    CatalogProperties.CATALOG_IMPL -> "org.apache.iceberg.rest.RESTCatalog",
    CatalogProperties.FILE_IO_IMPL -> s3CatalogFileIO.implClass,
    S3FileIOProperties.ENDPOINT -> s3CatalogFileIO.endpoint,
    S3FileIOProperties.PATH_STYLE_ACCESS -> s3CatalogFileIO.pathStyleEnabled,
    S3FileIOProperties.ACCESS_KEY_ID -> s3CatalogFileIO.accessKeyId,
    S3FileIOProperties.SECRET_ACCESS_KEY -> s3CatalogFileIO.secretAccessKey,
  ) ++ additionalProperties

  override implicit val catalogName: String = java.util.UUID.randomUUID.toString

  def initialize(): IcebergS3CatalogWriter =
    catalog.initialize(catalogName, catalogProperties.asJava)
    this
  
  override def delete(tableName: String): Future[Boolean] =
    val tableId = TableIdentifier.of(namespace, tableName)
    Future(catalog.dropTable(tableId))

  def append(data: Iterable[DataRow], name: String, schema: Schema): Future[Table] =
    val tableId = TableIdentifier.of(namespace, name)
    Future(catalog.loadTable(tableId)).flatMap(appendData(data, schema, false))

object IcebergS3CatalogWriter:
  /**
   * The ZLayer that creates the LazyOutputDataProcessor.
   */
  val layer: ZLayer[IcebergCatalogSettings, Nothing, CatalogWriter[RESTCatalog, Table, Schema]] =
    ZLayer {
      for
        settings <- ZIO.service[IcebergCatalogSettings]
      yield IcebergS3CatalogWriter(settings)
    }

  /**
   * Factory method to create IcebergS3CatalogWriter
   * @param namespace The namespace for the catalog
   * @param warehouse The warehouse location
   * @param catalogUri The catalog URI
   * @param additionalProperties Additional properties for the catalog
   * @param s3CatalogFileIO The S3 catalog file IO settings
   * @param locationOverride The location override for the catalog
   * @return The initialized IcebergS3CatalogWriter instance
   */
  def apply(namespace: String,
            warehouse: String,
            catalogUri: String,
            additionalProperties: Map[String, String],
            s3CatalogFileIO: S3CatalogFileIO,
            locationOverride: Option[String]): IcebergS3CatalogWriter =
    val writer = new IcebergS3CatalogWriter(
      namespace = namespace,
      warehouse = warehouse,
      catalogUri = catalogUri,
      additionalProperties = additionalProperties,
      s3CatalogFileIO = s3CatalogFileIO,
      locationOverride = locationOverride,
    )
    writer.initialize()
  
  /**
   * Factory method to create IcebergS3CatalogWriter
    * @param icebergSettings Iceberg settings
   * @return The initialized IcebergS3CatalogWriter instance
   */
  def apply(icebergSettings: IcebergCatalogSettings): IcebergS3CatalogWriter =
      IcebergS3CatalogWriter(
        namespace = icebergSettings.namespace,
        warehouse = icebergSettings.warehouse,
        catalogUri = icebergSettings.catalogUri,
        additionalProperties = icebergSettings.additionalProperties, 
        s3CatalogFileIO = icebergSettings.s3CatalogFileIO,
        locationOverride = icebergSettings.stagingLocation,
      )
