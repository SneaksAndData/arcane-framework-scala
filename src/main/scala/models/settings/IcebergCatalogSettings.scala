package com.sneaksanddata.arcane.framework
package models.settings

import services.iceberg.base.S3CatalogFileIO

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
   * Optional data location override for the table
   */
  val stagingLocation: Option[String]

  /**
   * Optional max rows per file. Default value is set by catalog writer
   */
  val maxRowsPerFile: Option[Int]
  
