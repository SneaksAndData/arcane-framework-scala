package com.sneaksanddata.arcane.framework
package tests.blobsource.json

import models.batches.BlobBatchCommons
import models.schemas.{DataRow, MergeKeyField}
import models.settings.sources.{SurrogateMergeKeyImpl, SurrogateMergeKey}
import services.blobsource.readers.listing.BlobListingJsonStreamingSource
import services.blobsource.versioning.BlobSourceWatermark
import services.naming.DefaultNameGenerator
import services.storage.models.s3.S3StoragePath
import tests.blobsource.json.JsonSourceSchemas.*
import tests.shared.S3StorageInfo.*
import tests.shared.{TestFieldSelectionRuleSettings, TestSinkSettings}

import zio.test.TestAspect.timeout
import zio.test.{Spec, TestAspect, TestEnvironment, ZIOSpecDefault, assertTrue}
import zio.{Chunk, Scope, ZIO}

def assertValidChunk(rows: Chunk[DataRow], expectedSize: Int, expectedFieldCount: Int) = {
  assertTrue(rows.size == expectedSize) && assertTrue(
    rows.forall(v => v.size == expectedFieldCount)
  )
}

object BlobListingJsonSourceTests extends ZIOSpecDefault:
  private val nameGenerator =
    new DefaultNameGenerator(
      sinkSettings = TestSinkSettings,
      backfillId = "",
      streamId = "blobsource_json_tests"
    )

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("BlobListingJsonSource")(
    test("getSchema returns correct schema") {
      for
        path      <- ZIO.succeed(S3StoragePath(s"s3a://$jsonBucket").get)
        shardPath <- ZIO.succeed(S3StoragePath("s3a://tmp").get)
        source <- ZIO.succeed(
          BlobListingJsonStreamingSource(
            path,
            shardPath,
            storageReader,
            nameGenerator,
            "/tmp",
            Seq("col0"),
            flatSchema,
            Some("/body"),
            Map(),
            TestFieldSelectionRuleSettings,
            Seq(SurrogateMergeKeyImpl(SurrogateMergeKey()))
          )
        )
        schema <- source.getSchema
      yield assertTrue(schema.size == 10 + 2) && assertTrue(
        schema.exists(f => f.name == MergeKeyField.name)
      ) && assertTrue(
        schema.exists(f => f.name == BlobBatchCommons.versionField.name)
      ) // expect 10 fields + ARCANE_MERGE_KEY + versionField
    },
    test("getChanges return correct rows") {
      for
        path      <- ZIO.succeed(S3StoragePath(s"s3a://$jsonBucket").get)
        shardPath <- ZIO.succeed(S3StoragePath("s3a://tmp").get)
        source <- ZIO.succeed(
          BlobListingJsonStreamingSource(
            path,
            shardPath,
            storageReader,
            nameGenerator,
            "/tmp",
            Seq("col0"),
            flatSchema,
            Some("/body"),
            Map(),
            TestFieldSelectionRuleSettings
          )
        )
        rows <- source.getChanges(BlobSourceWatermark.epoch).flatMap(_._1).runCollect
      yield assertValidChunk(rows, 50 * 100, 11)
    },
    test("getChanges return correct rows for source with variable number of fields") {
      for
        path      <- ZIO.succeed(S3StoragePath(s"s3a://$jsonBucketVariable").get)
        shardPath <- ZIO.succeed(S3StoragePath("s3a://tmp").get)
        source <- ZIO.succeed(
          BlobListingJsonStreamingSource(
            path,
            shardPath,
            storageReader,
            nameGenerator,
            "/tmp",
            Seq("col0"),
            flatSchema,
            Some("/body"),
            Map(),
            TestFieldSelectionRuleSettings
          )
        )
        rows <- source.getChanges(BlobSourceWatermark.epoch).flatMap(_._1).runCollect
      yield assertValidChunk(rows, 50 * 100, 11)
    },
    test("getChanges return correct rows when using array explode") {
      for
        path      <- ZIO.succeed(S3StoragePath(s"s3a://$jsonBucketNestedArray").get)
        shardPath <- ZIO.succeed(S3StoragePath("s3a://tmp").get)
        source <- ZIO.succeed(
          BlobListingJsonStreamingSource(
            path,
            shardPath,
            storageReader,
            nameGenerator,
            "/tmp",
            Seq("col0"),
            nestedArraySchema,
            Some("/body"),
            Map("/nested_array/value" -> Map()),
            TestFieldSelectionRuleSettings
          )
        )
        rows <- source.getChanges(BlobSourceWatermark.epoch).flatMap(_._1).runCollect
      yield assertValidChunk(rows, 50 * 100, 13)
    },
    test("getChanges return correct rows when using array explode for nested arrays") {
      for
        path      <- ZIO.succeed(S3StoragePath(s"s3a://$jsonBucketRootNestedArray").get)
        shardPath <- ZIO.succeed(S3StoragePath("s3a://tmp").get)
        source <- ZIO.succeed(
          BlobListingJsonStreamingSource(
            path,
            shardPath,
            storageReader,
            nameGenerator,
            "/tmp",
            Seq("col0"),
            nestedArraySchema,
            Some("/body"),
            Map("/data" -> Map(), "/nested_array/value" -> Map()),
            TestFieldSelectionRuleSettings
          )
        )
        rows <- source.getChanges(BlobSourceWatermark.epoch).flatMap(_._1).runCollect
      yield assertValidChunk(rows, 20 * 10 * 50, 13)
    },
    test("getChanges return correct rows when using array explode for nested arrays, when a root is JArray") {
      for
        path      <- ZIO.succeed(S3StoragePath(s"s3a://$jsonBucketRootNestedJArray").get)
        shardPath <- ZIO.succeed(S3StoragePath("s3a://tmp").get)
        source <- ZIO.succeed(
          BlobListingJsonStreamingSource(
            path,
            shardPath,
            storageReader,
            nameGenerator,
            "/tmp",
            Seq("col0"),
            nestedArraySchema,
            Some("/body"),
            Map("/nested_array/value" -> Map()),
            TestFieldSelectionRuleSettings
          )
        )
        rows <- source.getChanges(BlobSourceWatermark.epoch).flatMap(_._1).runCollect
      yield assertValidChunk(rows, 20 * 10 * 50, 13)
    }
  ) @@ timeout(zio.Duration.fromSeconds(60)) @@ TestAspect.withLiveClock
