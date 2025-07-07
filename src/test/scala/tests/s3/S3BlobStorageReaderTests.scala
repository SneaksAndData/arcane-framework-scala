package com.sneaksanddata.arcane.framework
package tests.s3

import services.storage.models.s3.S3StoragePath
import tests.shared.S3StorageInfo.storageReader

import zio.test.{Spec, TestEnvironment, ZIOSpecDefault, assertTrue}
import zio.{Scope, ZIO}

object S3BlobStorageReaderTests extends ZIOSpecDefault {
  val bucket = "s3-blob-reader"
  override def spec: Spec[TestEnvironment & Scope, Any] = suite("S3BlobStorageReader")(
    test("blobExists returns true if a blob exists in a bucket") {
      for
        path   <- ZIO.succeed(S3StoragePath(s"s3a://$bucket/0.parquet.gzip").get)
        result <- storageReader.blobExists(path)
      yield assertTrue(result)
    },
    test("blobExists returns false and doesn't error if a blob does not exist in a bucket") {
      for
        path   <- ZIO.succeed(S3StoragePath(s"s3a://$bucket/0.parquet.zip").get)
        result <- storageReader.blobExists(path)
      yield assertTrue(!result)
    },
    test("streamPrefixes returns a stream of correct length") {
      for
        path     <- ZIO.succeed(S3StoragePath(s"s3a://$bucket").get)
        prefixes <- storageReader.streamPrefixes(path).runCount
      yield assertTrue(prefixes == 50)
    }
  )
}
