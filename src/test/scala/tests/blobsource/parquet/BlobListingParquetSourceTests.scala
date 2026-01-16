package com.sneaksanddata.arcane.framework
package tests.blobsource.parquet

import models.batches.BlobBatchCommons
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
    test("getSchema returns correct schema with or without name mapping") {
      for
        path         <- ZIO.succeed(S3StoragePath(s"s3a://$bucket").get)
        source       <- ZIO.succeed(BlobListingParquetSource(path, storageReader, "/tmp", Seq("col0"), false))
        sourceMapped <- ZIO.succeed(BlobListingParquetSource(path, storageReader, "/tmp", Seq("col0"), true))
        schema       <- source.getSchema
        mappedSchema <- sourceMapped.getSchema
      yield assertTrue(schema.size == 11 + 2) && assertTrue(
        schema.exists(f => f.name == MergeKeyField.name)
      ) && assertTrue(
        schema.exists(f => f.name == BlobBatchCommons.versionField.name)
      ) // expect 11 fields + ARCANE_MERGE_KEY + versionField
        && assertTrue(schema == mappedSchema)
    },
    test("getChanges return correct rows") {
      for
        path   <- ZIO.succeed(S3StoragePath(s"s3a://$bucket").get)
        source <- ZIO.succeed(BlobListingParquetSource(path, storageReader, "/tmp", Seq("col0"), false))
        rows   <- source.getChanges(0).runCollect
      yield assertTrue(rows.size == 50 * 100) && assertTrue(rows.map(_._1).forall(v => v.size == 13)) && assertTrue(
        rows
          .map(_._1)
          .forall(row =>
            val pred = (row.takeRight(2).head.name == MergeKeyField.name) && (row
              .takeRight(2)
              .head
              .value
              .asInstanceOf[String] == Base64.getEncoder.encodeToString(
              MessageDigest.getInstance("SHA-256").digest(row.head.value.toString.getBytes("UTF-8"))
            ))

            if !pred then {
              println(
                s"Mismatch on ${row.takeRight(2).head.value}, key ${row.head.name} / value ${row.head.value}: expected ${Base64.getEncoder.encodeToString(MessageDigest.getInstance("SHA-256").digest(row.head.value.toString.getBytes("UTF-8")))}"
              )
            }

            pred
          )
      )
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
