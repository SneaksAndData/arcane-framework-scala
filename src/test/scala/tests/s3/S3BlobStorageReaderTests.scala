package com.sneaksanddata.arcane.framework
package tests.s3

import services.storage.models.s3.S3StoragePath
import tests.shared.S3StorageInfo.storageReader

import zio.test.{Spec, TestEnvironment, ZIOSpecDefault, assertTrue}
import zio.{Scope, ZIO}

object S3BlobStorageReaderTests extends ZIOSpecDefault {
  val bucket = "s3-blob-reader"
  override def spec: Spec[TestEnvironment & Scope, Any] = suite("S3BlobStorageReader")(
    test(s"correctly check if a blob exists in a container") {
      for
        path   <- ZIO.succeed(S3StoragePath(s"s3a://$bucket/0.parquet.zip").get)
        result <- storageReader.blobExists(path)
      yield assertTrue(result)
    }
  )
}
