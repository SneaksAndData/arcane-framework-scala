package com.sneaksanddata.arcane.framework
package tests.blobsource.json

import models.batches.BlobBatchCommons
import models.schemas.{DataRow, MergeKeyField}
import services.blobsource.readers.listing.BlobListingJsonSource
import services.storage.models.s3.S3StoragePath
import tests.blobsource.json.JsonSourceSchemas.*
import tests.shared.S3StorageInfo.*

import zio.test.TestAspect.timeout
import zio.test.{Spec, TestAspect, TestEnvironment, ZIOSpecDefault, assertTrue}
import zio.{Chunk, Scope, ZIO}

import java.security.MessageDigest
import java.util.Base64

def assertValidChunk(rows: Chunk[(DataRow, Long)], expectedSize: Int, expectedFieldCount: Int) = {
  assertTrue(rows.size == expectedSize) && assertTrue(
    rows.map(_._1).forall(v => v.size == expectedFieldCount)
  ) && assertTrue(
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

object BlobListingJsonSourceTests extends ZIOSpecDefault:
  override def spec: Spec[TestEnvironment & Scope, Any] = suite("BlobListingJsonSource")(
    test("getSchema returns correct schema") {
      for
        path <- ZIO.succeed(S3StoragePath(s"s3a://$jsonBucket").get)
        source <- ZIO.succeed(
          BlobListingJsonSource(path, storageReader, "/tmp", Seq("col0"), flatSchema, Some("/body"), Map())
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
        path <- ZIO.succeed(S3StoragePath(s"s3a://$jsonBucket").get)
        source <- ZIO.succeed(
          BlobListingJsonSource(path, storageReader, "/tmp", Seq("col0"), flatSchema, Some("/body"), Map())
        )
        rows <- source.getChanges(0).runCollect
      yield assertValidChunk(rows, 50 * 100, 12)
    },
    test("getChanges return correct rows for source with variable number of fields") {
      for
        path <- ZIO.succeed(S3StoragePath(s"s3a://$jsonBucketVariable").get)
        source <- ZIO.succeed(
          BlobListingJsonSource(path, storageReader, "/tmp", Seq("col0"), flatSchema, Some("/body"), Map())
        )
        rows <- source.getChanges(0).runCollect
      yield assertValidChunk(rows, 50 * 100, 12)
    },
    test("getChanges return correct rows when using array explode") {
      for
        path <- ZIO.succeed(S3StoragePath(s"s3a://$jsonBucketNestedArray").get)
        source <- ZIO.succeed(
          BlobListingJsonSource(
            path,
            storageReader,
            "/tmp",
            Seq("col0"),
            nestedArraySchema,
            Some("/body"),
            Map("/nested_array/value" -> Map())
          )
        )
        rows <- source.getChanges(0).runCollect
      yield assertValidChunk(rows, 50 * 100, 14)
    },
    test("getChanges return correct rows when using array explode for nested arrays") {
      for
        path <- ZIO.succeed(S3StoragePath(s"s3a://$jsonBucketRootNestedArray").get)
        source <- ZIO.succeed(
          BlobListingJsonSource(
            path,
            storageReader,
            "/tmp",
            Seq("col0"),
            nestedArraySchema,
            Some("/body"),
            Map("/data" -> Map(), "/nested_array/value" -> Map())
          )
        )
        rows <- source.getChanges(0).runCollect
      yield assertValidChunk(rows, 20 * 10 * 50, 14)
    }
  ) @@ timeout(zio.Duration.fromSeconds(20)) @@ TestAspect.withLiveClock
