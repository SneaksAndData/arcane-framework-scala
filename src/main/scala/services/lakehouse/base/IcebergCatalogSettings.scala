package com.sneaksanddata.arcane.framework
package services.lakehouse.base

import services.lakehouse.S3CatalogFileIO

/**
 * Represents the settings of an Iceberg catalog.
 */
trait IcebergCatalogSettings:
  /**
   * The namespace of the catalog.
   */
  val namespace: String
  
  /**
   * The warehouse name of the catalog.
   */
  val warehouse: String
  
  /**
   * The catalog server URI.
   */
  val catalogUri: String
  
  /**
   * The catalog additional properties.
   */
  val additionalProperties: Map[String, String]
  
  /**
   * The catalog S3 properties.
   */
  val s3CatalogFileIO: S3CatalogFileIO
  
  /**
   * The lakehouse location of the catalog
   */
  val stagingLocation: Option[String]
  
