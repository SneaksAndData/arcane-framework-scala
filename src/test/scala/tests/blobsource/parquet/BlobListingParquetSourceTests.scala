package com.sneaksanddata.arcane.framework
package tests.blobsource.parquet

import models.schemas.MergeKeyField
import services.blobsource.readers.listing.BlobListingParquetSource
import services.storage.models.s3.S3StoragePath
import tests.shared.S3StorageInfo.*

import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, ZIO}

import java.security.MessageDigest
import java.util.Base64

object BlobListingParquetSourceTests extends ZIOSpecDefault:
  override def spec: Spec[TestEnvironment & Scope, Any] = suite("BlobListingParquetSource")(
    test("getSchema returns correct schema") {
      for
        path   <- ZIO.succeed(S3StoragePath(s"s3a://$bucket").get)
        source <- ZIO.succeed(BlobListingParquetSource(path, storageReader, "/tmp", Seq("col0")))
        schema <- source.getSchema
      yield assertTrue(schema.size == 10 + 2) && assertTrue(
        schema.exists(f => f.name == MergeKeyField.name)
      ) // expect 10 fields + ARCANE_MERGE_KEY + versionField
    },
    test("getChanges return correct rows") {
      for
        path   <- ZIO.succeed(S3StoragePath(s"s3a://$bucket").get)
        source <- ZIO.succeed(BlobListingParquetSource(path, storageReader, "/tmp", Seq("col0")))
        rows   <- source.getChanges(0).runCollect
      yield assertTrue(rows.size == 50 * 100) && assertTrue(rows.map(_._1).forall(v => v.size == 11)) && assertTrue(
        rows
          .map(_._1)
          .forall(row =>
            (row.last.name == MergeKeyField.name) && (row.last.value
              .asInstanceOf[String] == Base64.getEncoder.encodeToString(
              MessageDigest.getInstance("SHA-256").digest(row.head.value.toString.getBytes("UTF-8"))
            ))
          )
      )
    }
  ) @@ timeout(zio.Duration.fromSeconds(10)) @@ TestAspect.withLiveClock
