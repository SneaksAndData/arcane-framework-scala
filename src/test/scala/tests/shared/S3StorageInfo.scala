package com.sneaksanddata.arcane.framework
package tests.shared

import services.storage.models.s3.S3ClientSettings
import services.storage.services.s3.S3BlobStorageReader

object S3StorageInfo:
  val bucket          = "s3-blob-reader"
  val accessKeyId     = "minioadmin"
  val secretAccessKey = "minioadmin"
  val endpoint        = "http://localhost:9000"

  val storageReader = S3BlobStorageReader(
    Some(secretAccessKey),
    Some(accessKeyId),
    Some(
      S3ClientSettings(
        region = Some("us-east-1"),
        endpoint = Some(endpoint),
        pathStyleAccess = true,
        maxResultsPerPage = 5
      )
    )
  )
