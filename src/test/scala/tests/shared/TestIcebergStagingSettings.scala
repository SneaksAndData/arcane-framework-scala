package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.iceberg.IcebergStagingSettings
import services.iceberg.base.S3CatalogFileIO

object TestIcebergStagingSettings extends IcebergStagingSettings:
  /** The namespace of the catalog.
    */
  override val namespace: String = "namespace"

  /** The warehouse name of the catalog.
    */
  override val warehouse: String = "warehouse"

  /** The catalog server URI.
    */
  override val catalogUri: String = "http://localhost:8080"

  /** The catalog S3 properties.
    */
  override val s3CatalogFileIO: S3CatalogFileIO = S3CatalogFileIO

  /** The lakehouse location of the catalog
    */
  override val stagingLocation: Option[String] = None

  override val maxRowsPerFile: Option[Int] = None

  override val additionalProperties: Map[String, String] = S3CatalogFileIO.properties
