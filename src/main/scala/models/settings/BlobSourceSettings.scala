package com.sneaksanddata.arcane.framework
package models.settings

/** Synapse-link specific source settings
  */
trait BlobSourceSettings extends SourceSettings:

  /**
   * Blob storage prefix for source blobs
   */
  val sourcePath: String

  /** Location to store temporary files
    */
  val tempStoragePath: String

  /** Primary keys for external blob data rows
    */
  val primaryKeys: List[String]
