package com.sneaksanddata.arcane.framework
package tests.blobsource.json

import models.batches.BlobBatchCommons
import models.schemas.MergeKeyField
import services.blobsource.readers.listing.BlobListingJsonSource
import services.storage.models.s3.S3StoragePath
import tests.shared.S3StorageInfo.*

import zio.test.TestAspect.timeout
import zio.test.{Spec, TestAspect, TestEnvironment, ZIOSpecDefault, assertTrue}
import zio.{Scope, ZIO}

import java.security.MessageDigest
import java.util.Base64

object BlobListingJsonSourceTests extends ZIOSpecDefault:
  private val testSchema = """{
                             |    "name": "BlobListingJsonSource",
                             |    "namespace": "com.sneaksanddata.arcane.BlobListingJsonSource",
                             |    "doc": "Avro Schema for BlobListingJsonSource tests",
                             |    "type": "record",
                             |    "fields": [
                             |        {
                             |            "name": "col0",
                             |            "type": "int"
                             |        },
                             |        {
                             |            "name": "col1",
                             |            "type": "string"
                             |        },
                             |        {
                             |            "name": "col2",
                             |            "type": "int"
                             |        },
                             |        {
                             |            "name": "col3",
                             |            "type": "string"
                             |        },
                             |        {
                             |            "name": "col4",
                             |            "type": "int"
                             |        },
                             |        {
                             |            "name": "col5",
                             |            "type": "string"
                             |        },
                             |        {
                             |            "name": "col6",
                             |            "type": "int"
                             |        },
                             |        {
                             |            "name": "col7",
                             |            "type": "string"
                             |        },
                             |        {
                             |            "name": "col8",
                             |            "type": "int"
                             |        },
                             |        {
                             |            "name": "col9",
                             |            "type": "string"
                             |        }
                             |    ]
                             |}""".stripMargin
  override def spec: Spec[TestEnvironment & Scope, Any] = suite("BlobListingJsonSource")(
    test("getSchema returns correct schema") {
      for
        path   <- ZIO.succeed(S3StoragePath(s"s3a://$jsonBucket").get)
        source <- ZIO.succeed(BlobListingJsonSource(path, storageReader, "/tmp", Seq("col0"), testSchema))
        schema <- source.getSchema
      yield assertTrue(schema.size == 10 + 2) && assertTrue(
        schema.exists(f => f.name == MergeKeyField.name)
      ) && assertTrue(
        schema.exists(f => f.name == BlobBatchCommons.versionField.name)
      ) // expect 10 fields + ARCANE_MERGE_KEY + versionField
    },
    test("getChanges return correct rows") {
      for
        path   <- ZIO.succeed(S3StoragePath(s"s3a://$jsonBucket").get)
        source <- ZIO.succeed(BlobListingJsonSource(path, storageReader, "/tmp", Seq("col0"), testSchema))
        rows   <- source.getChanges(0).runCollect
      yield assertTrue(rows.size == 50 * 100) && assertTrue(rows.map(_._1).forall(v => v.size == 12)) && assertTrue(
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
  ) @@ timeout(zio.Duration.fromSeconds(20)) @@ TestAspect.withLiveClock
