package com.sneaksanddata.arcane.framework
package tests.pullstream

import models.ddl.CreateTableRequest as IcebergCreateTableRequest
import models.schemas.MergeKeyField
import services.pullstream.PullStreamingSource
import services.pullstream.versioning.PullStreamWatermark
import tests.pullstream.util.PullStreamTestServices
import tests.shared.{IcebergUtil, TestDynamicSinkSettings}
import com.sneaksanddata.arcane.framework.services.iceberg.given_Conversion_ArcaneSchema_Schema

import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, ZIO}

import java.time.{Instant, OffsetDateTime, ZoneOffset}

object PullStreamSourceTest extends ZIOSpecDefault:

  private val schema = PullStreamTestServices.payloadSchema

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("PullStreamTests")(
    test("DetectHasRows") {
      for {
        tableName <- PullStreamTestServices.genSourceTableName
        icebergUtil = IcebergUtil(TestDynamicSinkSettings(tableName).icebergCatalog)
        client <- PullStreamTestServices.getClient
        result <- PullStreamTestServices.withSourceTable(tableName, client) {
          for {
            sinkPropertyManager <- icebergUtil.getSinkTablePropertyManager
            source <- ZIO.succeed(
              PullStreamingSource(
                settings = PullStreamTestServices
                  .pullStreamSettings(tableName),
                dynamodbClient = client,
                sinkPropertyManager = sinkPropertyManager,
                targetTableFullName = s"testWarehouse.testNs.$tableName",
                pageSize = Some(1000)
              )
            )
            _ <- PullStreamTestServices.insertMany(client, tableName, count = 1)
            hasRows <- source.hasRows(
              PullStreamWatermark(OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC))
            )
          } yield assertTrue(hasRows)
        }
      } yield result
    },
    test("PaginatesLargeChangeSet") {
      // Insert more items than the configured page size and verify that getChanges follows
      // LastEvaluatedKey across pages and emits every DataRow exactly once, in watermark order.
      val totalItems = 25
      val pageSize   = Some(7) // forces at least 4 pages: 7 + 7 + 7 + 4
      for {
        tableName <- PullStreamTestServices.genSourceTableName
        targetTableName = s"wh.ns.$tableName"
        icebergUtil     = IcebergUtil(TestDynamicSinkSettings(targetTableName).icebergCatalog)
        client <- PullStreamTestServices.getClient
        result <- PullStreamTestServices.withSourceTable(tableName, client) {
          for {
            sinkEntityManager   <- icebergUtil.getSinkEntityManager
            _                   <- sinkEntityManager.createTable(IcebergCreateTableRequest(tableName, schema, true))
            sinkPropertyManager <- icebergUtil.getSinkTablePropertyManager
            source <- ZIO.succeed(
              PullStreamingSource(
                settings = PullStreamTestServices.pullStreamSettings(tableName),
                dynamodbClient = client,
                sinkPropertyManager = sinkPropertyManager,
                targetTableFullName = s"testWarehouse.testNs.$tableName",
                pageSize = pageSize
              )
            )
            _ <- PullStreamTestServices.insertMany(client, tableName, count = totalItems)
            changes <- source
              .getChanges(PullStreamWatermark(OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC)))
              .runCollect
            (rowStream, _) = changes.head
            rows <- rowStream.runCollect
            userIds = rows.map(
              _.find(_.name == "userId").flatMap(f => Option(f.value)).map(_.toString).getOrElse("")
            )
          } yield
            // outer stream still emits a single (rows, schema) pair regardless of underlying pages
            assertTrue(changes.length == 1)
            // every inserted item surfaces as exactly one row (one row per payload)
              && assertTrue(rows.length == totalItems)
              // row shape matches the schema (same field names, in order)
              && assertTrue(rows.head.map(_.name) == schema.map(_.name).toList :+ MergeKeyField.name)
              // ordering follows the DynamoDB sort key (ascending by default) so we see user-0 .. user-N
              && assertTrue(userIds.toList == (0 until totalItems).map(i => s"user-$i").toList)
        }
      } yield result
    },
    test("AppendsWatermarkAttributeToRows") {
      // The watermark is an attribute of the DynamoDB item and is deliberately absent from `payload`.
      // When the sink declares a column for it, its value must be taken from the item and written to the row.
      val totalItems = 3
      val startAt    = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC).plusHours(1)
      for {
        tableName <- PullStreamTestServices.genSourceTableName
        targetTableName = s"wh.ns.$tableName"
        icebergUtil     = IcebergUtil(TestDynamicSinkSettings(targetTableName).icebergCatalog)
        client <- PullStreamTestServices.getClient
        result <- PullStreamTestServices.withSourceTable(tableName, client) {
          for {
            sinkEntityManager <- icebergUtil.getSinkEntityManager
            _ <- sinkEntityManager.createTable(
              IcebergCreateTableRequest(tableName, PullStreamTestServices.watermarkedPayloadSchema, true)
            )
            sinkPropertyManager <- icebergUtil.getSinkTablePropertyManager
            source = PullStreamingSource(
              settings = PullStreamTestServices.pullStreamSettings(tableName),
              dynamodbClient = client,
              sinkPropertyManager = sinkPropertyManager,
              targetTableFullName = s"testWarehouse.testNs.$tableName",
              pageSize = None
            )
            _ <- PullStreamTestServices.insertMany(client, tableName, count = totalItems, startAt = startAt)
            changes <- source
              .getChanges(PullStreamWatermark(OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC)))
              .runCollect
            (rowStream, _) = changes.head
            rows <- rowStream.runCollect
            watermarks = rows.map(
              _.find(_.name == PullStreamTestServices.watermarkField)
                .flatMap(cell => Option(cell.value))
                .map(_.toString)
                .getOrElse("")
            )
            expected = (0 until totalItems).map(i => startAt.plusSeconds(i.toLong).toString).toList
          } yield assertTrue(rows.length == totalItems)
            && assertTrue(watermarks.toList == expected)
            // the payload fields must survive alongside the appended watermark
            && assertTrue(
              rows.head.map(_.name).toSet ==
                PullStreamTestServices.watermarkedPayloadSchema.map(_.name).toSet + MergeKeyField.name
            )
            && assertTrue(source.versionFieldName == PullStreamTestServices.watermarkField)
        }
      } yield result
    },
    test("OmitsWatermarkWhenSinkHasNoSuchColumn") {
      // Sinks that do not declare the column must keep working: strict decoding would otherwise reject every row.
      val totalItems = 2
      for {
        tableName <- PullStreamTestServices.genSourceTableName
        targetTableName = s"wh.ns.$tableName"
        icebergUtil     = IcebergUtil(TestDynamicSinkSettings(targetTableName).icebergCatalog)
        client <- PullStreamTestServices.getClient
        result <- PullStreamTestServices.withSourceTable(tableName, client) {
          for {
            sinkEntityManager   <- icebergUtil.getSinkEntityManager
            _                   <- sinkEntityManager.createTable(IcebergCreateTableRequest(tableName, schema, true))
            sinkPropertyManager <- icebergUtil.getSinkTablePropertyManager
            source = PullStreamingSource(
              settings = PullStreamTestServices.pullStreamSettings(tableName),
              dynamodbClient = client,
              sinkPropertyManager = sinkPropertyManager,
              targetTableFullName = s"testWarehouse.testNs.$tableName",
              pageSize = None
            )
            _ <- PullStreamTestServices.insertMany(client, tableName, count = totalItems)
            changes <- source
              .getChanges(PullStreamWatermark(OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC)))
              .runCollect
            (rowStream, _) = changes.head
            rows <- rowStream.runCollect
          } yield assertTrue(rows.length == totalItems)
            && assertTrue(rows.head.map(_.name) == schema.map(_.name).toList :+ MergeKeyField.name)
        }
      } yield result
    },
    test("ReadsJsonPointerFromSinkTableProperty") {
      // The producing service derives a table's columns from a JSON pointer into the pushed document and publishes
      // that pointer on the table, because the stream's own settings have no field to carry it. Reading it back is
      // the only thing that lets the nested document be decoded against the columns derived from it.
      val totalItems = 2
      for {
        tableName <- PullStreamTestServices.genSourceTableName
        targetTableName = s"wh.ns.$tableName"
        icebergUtil     = IcebergUtil(TestDynamicSinkSettings(targetTableName).icebergCatalog)
        client <- PullStreamTestServices.getClient
        result <- PullStreamTestServices.withSourceTable(tableName, client) {
          for {
            sinkEntityManager <- icebergUtil.getSinkEntityManager
            _ <- sinkEntityManager.createTable(
              IcebergCreateTableRequest(tableName, PullStreamTestServices.pointedPayloadSchema, true)
            )
            sinkPropertyManager <- icebergUtil.getSinkTablePropertyManager
            _ <- sinkPropertyManager.setProperty(
              tableName,
              PullStreamingSource.jsonPointerPropertyName,
              "/payload"
            )
            source = PullStreamingSource(
              // deliberately left out of the settings: the table is the only place the pointer comes from
              settings = PullStreamTestServices.pullStreamSettings(tableName),
              dynamodbClient = client,
              sinkPropertyManager = sinkPropertyManager,
              targetTableFullName = s"testWarehouse.testNs.$tableName",
              pageSize = None
            )
            _ <- PullStreamTestServices.insertMany(
              client,
              tableName,
              count = totalItems,
              payload = PullStreamTestServices.productionPayload
            )
            changes <- source
              .getChanges(PullStreamWatermark(OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC)))
              .runCollect
            (rowStream, _) = changes.head
            rows <- rowStream.runCollect
            eventTypes = rows.flatMap(_.find(_.name == "eventType")).map(_.value.toString)
          } yield
            // the envelope around the pointed node contributes no columns, so decoding could only have started
            // below the pointer
            assertTrue(rows.length == totalItems)
              && assertTrue(
                rows.head.map(_.name) == PullStreamTestServices.pointedPayloadSchema.map(_.name).toList
                  :+ MergeKeyField.name
              )
              && assertTrue(eventTypes.forall(_ == "Producer1Event"))
        }
      } yield result
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock @@ TestAspect.withLiveRandom
