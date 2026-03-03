package com.sneaksanddata.arcane.framework
package models.settings.iceberg

/** Represents the settings of an Iceberg catalog.
  */
trait IcebergStagingSettings extends IcebergCatalogSettings:
  /** Optional max rows per file. Default value is set by catalog writer
    */
  val maxRowsPerFile: Option[Int]
