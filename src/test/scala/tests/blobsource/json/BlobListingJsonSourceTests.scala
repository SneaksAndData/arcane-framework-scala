package com.sneaksanddata.arcane.framework
package tests.blobsource.json

import models.batches.BlobBatchCommons
import models.schemas.{DataRow, MergeKeyField, VersionField}
import services.blobsource.readers.listing.BlobListingJsonStreamingSource
import services.blobsource.versioning.BlobSourceWatermark
import services.naming.DefaultNameGenerator
import services.storage.models.s3.S3StoragePath
import tests.blobsource.json.JsonSourceSchemas.*
import tests.shared.S3StorageInfo.*
import tests.shared.{TestFieldSelectionRuleSettings, TestSinkSettings}
import utils.HashUtils

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
            Seq.empty
          )
        )
        schema <- source.getSchema
      yield assertTrue(schema.size == 10 + 3) && assertTrue(
        schema.exists(f => f.name == MergeKeyField.name)
      ) && assertTrue(
        schema.exists(f => f.name == BlobBatchCommons.versionField.name)
      ) && assertTrue(
        schema.exists(f => f.name == VersionField.name)
      ) // expect 10 fields + source version + configured Arcane fields
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
            TestFieldSelectionRuleSettings,
            Seq.empty
          )
        )
        rows <- source.getChanges(BlobSourceWatermark.epoch).flatMap(_._1).runCollect
      yield assertValidChunk(rows, 50 * 100, 13)
    },
    test("getChanges adds required merge key and version fields") {
      val pkColumns = Seq("col1", "col3")

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
            pkColumns,
            flatSchema,
            Some("/body"),
            Map(),
            TestFieldSelectionRuleSettings,
            Seq.empty
          )
        )
        rows <- source.getChanges(BlobSourceWatermark.epoch).flatMap(_._1).runCollect
      yield assertValidChunk(rows, 50 * 100, 13) && assertTrue(
        rows.forall { row =>
          val primaryKeyValues = pkColumns.map(name => row.find(_.name == name).map(_.value))
          val mergeKey         = row.find(_.name == MergeKeyField.name).map(_.value)
          val sourceVersion    = row.find(_.name == BlobBatchCommons.versionField.name).map(_.value)
          val arcaneVersion    = row.find(_.name == VersionField.name).map(_.value)

          (primaryKeyValues, mergeKey) match
            case (Seq(Some(first: CharSequence), Some(second: CharSequence)), Some(value: String)) =>
              value == HashUtils.murmur3(s"$first#$second") && arcaneVersion == sourceVersion
            case _ => false
        }
      )
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
            TestFieldSelectionRuleSettings,
            Seq.empty
          )
        )
        rows <- source.getChanges(BlobSourceWatermark.epoch).flatMap(_._1).runCollect
      yield assertValidChunk(rows, 50 * 100, 13)
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
            Seq("nested_col_1"),
            nestedArraySchema,
            Some("/body"),
            Map("/nested_array/value" -> Map()),
            TestFieldSelectionRuleSettings,
            Seq.empty
          )
        )
        rows <- source.getChanges(BlobSourceWatermark.epoch).flatMap(_._1).runCollect
      yield assertValidChunk(rows, 50 * 100, 15)
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
            Seq("nested_col_1"),
            nestedArraySchema,
            Some("/body"),
            Map("/data" -> Map(), "/nested_array/value" -> Map()),
            TestFieldSelectionRuleSettings,
            Seq.empty
          )
        )
        rows <- source.getChanges(BlobSourceWatermark.epoch).flatMap(_._1).runCollect
      yield assertValidChunk(rows, 20 * 10 * 50, 15)
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
            Seq("nested_col_1"),
            nestedArraySchema,
            Some("/body"),
            Map("/nested_array/value" -> Map()),
            TestFieldSelectionRuleSettings,
            Seq.empty
          )
        )
        rows <- source.getChanges(BlobSourceWatermark.epoch).flatMap(_._1).runCollect
      yield assertValidChunk(rows, 20 * 10 * 50, 15)
    }
  ) @@ timeout(zio.Duration.fromSeconds(60)) @@ TestAspect.withLiveClock
