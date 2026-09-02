package com.sneaksanddata.arcane.framework
package tests.blobsource.parquet

import models.batches.BlobBatchCommons
import models.schemas.{MergeKeyField, VersionField}
import services.blobsource.readers.listing.BlobListingParquetStreamingSource
import services.blobsource.versioning.BlobSourceWatermark
import services.naming.DefaultNameGenerator
import services.storage.models.s3.S3StoragePath
import tests.shared.S3StorageInfo.*
import tests.shared.TestDataRowModifications.mergeModifications
import tests.shared.{TestFieldSelectionRuleSettings, TestSinkSettings}

import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, ZIO}

object BlobListingParquetSourceTests extends ZIOSpecDefault:
  private val nameGenerator =
    new DefaultNameGenerator(
      sinkSettings = TestSinkSettings,
      backfillId = "",
      streamId = "blobsource_parquet_tests"
    )

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("BlobListingParquetSource")(
    test("getSchema returns correct schema with or without name mapping") {
      for
        path      <- ZIO.succeed(S3StoragePath(s"s3a://$bucket").get)
        shardPath <- ZIO.succeed(S3StoragePath("s3a://tmp").get)
        source <- ZIO.succeed(
          BlobListingParquetStreamingSource(
            path,
            shardPath,
            storageReader,
            nameGenerator,
            "/tmp",
            Seq("col1"),
            false,
            None,
            TestFieldSelectionRuleSettings,
            mergeModifications
          )
        )
        sourceMapped <- ZIO.succeed(
          BlobListingParquetStreamingSource(
            path,
            shardPath,
            storageReader,
            nameGenerator,
            "/tmp",
            Seq("col1"),
            true,
            None,
            TestFieldSelectionRuleSettings,
            mergeModifications
          )
        )
        schema       <- source.getSchema
        mappedSchema <- sourceMapped.getSchema
      yield assertTrue(schema.size == 11 + 3) && assertTrue(
        schema.exists(f => f.name == MergeKeyField.name)
      ) && assertTrue(
        schema.exists(f => f.name == BlobBatchCommons.versionField.name)
      ) && assertTrue(
        schema.exists(f => f.name == VersionField.name)
      ) // expect 11 fields + source version + configured Arcane fields
        && assertTrue(schema == mappedSchema)
    },
    test("getChanges return correct rows") {
      for
        path      <- ZIO.succeed(S3StoragePath(s"s3a://$bucket").get)
        shardPath <- ZIO.succeed(S3StoragePath("s3a://tmp").get)
        source <- ZIO.succeed(
          BlobListingParquetStreamingSource(
            path,
            shardPath,
            storageReader,
            nameGenerator,
            "/tmp",
            Seq("col1"),
            false,
            None,
            TestFieldSelectionRuleSettings,
            mergeModifications
          )
        )
        rows <- source.getChanges(BlobSourceWatermark.epoch).flatMap(_._1).runCollect
      yield assertTrue(rows.size == 50 * 100) && assertTrue(rows.forall(v => v.size == 14)) && assertTrue(
        rows.forall { row =>
          row.find(_.name == VersionField.name).map(_.value) ==
            row.find(_.name == BlobBatchCommons.versionField.name).map(_.value)
        }
      )
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
