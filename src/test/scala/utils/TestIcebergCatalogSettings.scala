package com.sneaksanddata.arcane.framework
package utils

import services.lakehouse.base.{IcebergCatalogSettings, S3CatalogFileIO}

object TestIcebergCatalogSettings extends IcebergCatalogSettings:
  /**
   * The namespace of the catalog.
   */
  override val namespace: String = "namespace"
  /**
   * The warehouse name of the catalog.
   */
  override val warehouse: String = "warehouse"
  /**
   * The catalog server URI.
   */
  override val catalogUri: String = "http://localhost:8080"
  /**
   * The catalog additional properties.
   */
  override val additionalProperties: Map[String, String] = Map()
  /**
   * The catalog S3 properties.
   */
  override val s3CatalogFileIO: S3CatalogFileIO = S3CatalogFileIO
  /**
   * The lakehouse location of the catalog
   */
  override val stagingLocation: Option[String] = None
