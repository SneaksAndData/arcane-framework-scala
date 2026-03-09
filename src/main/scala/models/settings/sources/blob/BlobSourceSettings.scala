package com.sneaksanddata.arcane.framework
package models.settings.sources.blob

import models.settings.sources.SourceSettings

import com.sneaksanddata.arcane.framework.services.storage.models.s3.S3ClientSettings

/** Blob-source specific source settings
  */
trait BlobSourceSettings extends SourceSettings:

  /** Blob storage prefix for source blobs
    */
  val sourcePath: String

  /** Location to store temporary files
    */
  val tempStoragePath: String

  /** Primary keys for external blob data rows
    */
  val primaryKeys: List[String]

  /** Optional s3 client settings
    */
  val s3ClientSettings: S3ClientSettings
