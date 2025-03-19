package com.sneaksanddata.arcane.framework
package services.lakehouse

import services.lakehouse.base.{CatalogWriterBuilder, IcebergCatalogSettings, S3CatalogFileIO}

import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import zio.{ZIO, ZLayer}

/**
 * IcebergS3CatalogWriterBuilder is a builder class that creates an IcebergS3CatalogWriter instance
 *
 * @param namespace The namespace of the catalog
 * @param warehouse The warehouse name of the catalog
 * @param catalogUri The catalog server URI
 * @param additionalProperties The catalog additional properties
 * @param s3CatalogFileIO The catalog S3 properties
 * @param locationOverride The lakehouse location of the catalog
 */
class IcebergS3CatalogWriterBuilder(namespace: String,
                                    warehouse: String,
                                    catalogUri: String,
                                    additionalProperties: Map[String, String],
                                    s3CatalogFileIO: S3CatalogFileIO,
                                    locationOverride: Option[String] = None) extends CatalogWriterBuilder[RESTCatalog, Table, Schema]:

  /**
   * Creates an IcebergS3CatalogWriter instance
   *  @return CatalogWriter instance ready to perform data operations
   */
  def initialize(): IcebergS3CatalogWriter =
    IcebergS3CatalogWriter(namespace, warehouse, catalogUri, additionalProperties, s3CatalogFileIO, locationOverride).initialize()

/**
 * IcebergS3CatalogWriterBuilder campaign object
 */
object IcebergS3CatalogWriterBuilder:

  /**
   * The environment type for the ZLayer
   */
  type Environment = IcebergCatalogSettings

  /**
   * Factory method to create IcebergS3CatalogWriter
   *
   * @param icebergSettings Iceberg settings
   * @return The initialized IcebergS3CatalogWriter instance
   */
  def apply(icebergSettings: IcebergCatalogSettings): IcebergS3CatalogWriterBuilder =
    new IcebergS3CatalogWriterBuilder(
      icebergSettings.namespace,
      icebergSettings.warehouse,
      icebergSettings.catalogUri,
      icebergSettings.additionalProperties,
      icebergSettings.s3CatalogFileIO,
      icebergSettings.stagingLocation,
    )

  /**
   * The ZLayer that creates the LazyOutputDataProcessor.
   */
  val layer: ZLayer[Environment, Throwable, CatalogWriterBuilder[RESTCatalog, Table, Schema]] =
    ZLayer {
      for
        settings <- ZIO.service[IcebergCatalogSettings]
      yield IcebergS3CatalogWriterBuilder(settings)
    }