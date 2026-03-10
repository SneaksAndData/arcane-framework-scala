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

  override val additionalProperties: Map[String, String] = S3CatalogFileIO.properties
