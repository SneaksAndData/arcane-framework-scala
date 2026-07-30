package com.sneaksanddata.arcane.framework
package tests.pullstream

import models.ddl.CreateTableRequest as IcebergCreateTableRequest
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
                targetTableName = s"testWarehouse.testNs.$tableName",
                pageSize = Some(1000),
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
                targetTableName = s"testWarehouse.testNs.$tableName",
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
            && assertTrue(rows.head.map(_.name) == schema.map(_.name).toList)
            // ordering follows the DynamoDB sort key (ascending by default) so we see user-0 .. user-N
            && assertTrue(userIds.toList == (0 until totalItems).map(i => s"user-$i").toList)
        }
      } yield result
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock @@ TestAspect.withLiveRandom
