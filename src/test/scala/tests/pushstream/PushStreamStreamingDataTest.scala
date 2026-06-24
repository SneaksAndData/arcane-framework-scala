package com.sneaksanddata.arcane.framework
package tests.pushstream

import models.ddl.CreateTableRequest as IcebergCreateTableRequest
import models.schemas.{ArcaneSchema, ArcaneType, Field}
import models.settings.backfill.BackfillBehavior.Overwrite
import models.settings.backfill.{BackfillBehavior, BackfillSettings}
import models.settings.streaming.{ChangeCaptureSettings, StreamModeSettings}
import services.iceberg.SchemaConversions.toIcebergSchema
import services.metrics.DeclaredMetrics
import services.pushstream.{PushStreamSourceDataProvider, PushStreamStreamingDataProvider, PushStreamingSource}
import services.pushstream.versioning.PushStreamWatermark
import tests.shared.*

import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, PutItemRequest, PutItemResponse}
import zio.test.TestAspect.timeout
import zio.test.{Spec, TestAspect, TestEnvironment, ZIOSpecDefault, assertTrue}
import zio.{Scope, Task, ZIO}

import java.time.{Duration, Instant, OffsetDateTime, ZoneOffset}
import scala.jdk.CollectionConverters.*
import scala.language.postfixOps

object PushStreamStreamingDataProviderTests extends ZIOSpecDefault:

  private val defaultStreamMode = new StreamModeSettings:
    override val backfill: BackfillSettings = new BackfillSettings:
      override val backfillBehavior: BackfillBehavior = Overwrite
      override val backfillStartDate: Option[OffsetDateTime] =
        Some(OffsetDateTime.now(ZoneOffset.UTC).minus(Duration.ofHours(12)))

    override val changeCapture: ChangeCaptureSettings = new ChangeCaptureSettings:
      override val changeCaptureInterval: Duration     = Duration.ofSeconds(1)
      override val changeCaptureJitterVariance: Double = 0.0001
      override val changeCaptureJitterSeed: Long       = 0

  private val sourceTableName     = "pushstream_streaming_test"
  private val targetTableName     = s"demo.test.$sourceTableName"
  private val primaryKeyField     = "producer"
  private val primaryKeyValue     = "producer1"
  private val watermarkField      = "timestampUTC"
  private val defaultSinkSettings = TestDynamicSinkSettings(targetTableName)
  private val icebergUtil         = IcebergUtil(defaultSinkSettings.icebergCatalog)

  // schema must match the JSON payload below
  private val schema = ArcaneSchema(
    Seq(
      Field("userId", ArcaneType.StringType),
      Field("level", ArcaneType.StringType)
    )
  )

  private def insertOne(
      client: DynamoDbClient,
      tableName: String,
      timestamp: OffsetDateTime,
      userId: String,
      level: String
  ): Task[PutItemResponse] =
    val item = Map(
      primaryKeyField -> AttributeValue.builder().s(primaryKeyValue).build(),
      watermarkField  -> AttributeValue.builder().s(timestamp.toString).build(),
      // Avro JSON encoding for optional fields requires the type-tag wrapping
      "payload" -> AttributeValue
        .builder()
        .s(s"""{"userId":{"string":"$userId"},"level":{"string":"$level"}}""")
        .build(),
      "schemaId" -> AttributeValue.builder().n("1").build()
    ).asJava
    ZIO.attemptBlocking(
      client.putItem(PutItemRequest.builder().tableName(tableName).item(item).build())
    )

  private def insertRows(client: DynamoDbClient, tableName: String, n: Int): Task[Unit] =
    val base = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC).plusHours(1)
    ZIO
      .foreach(1 to n) { i =>
        insertOne(client, tableName, base.plusSeconds(i.toLong), s"user-$i", "user")
      }
      .unit

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("PushStreamStreamingDataProviderTests") {
    test("returns correct number of rows while streaming") {
      val numberRowsToTake = 5
      for
        client <- PushStreamTestServices.getClient
        result <- ZIO.acquireReleaseWith(
          PushStreamTestServices.createTable(sourceTableName, client)
        )(_ => PushStreamTestServices.deleteTable(client, sourceTableName).orDie) { _ =>
          for
            // seed source DDB table with more than `numberRowsToTake` rows
            _                 <- insertRows(client, sourceTableName, numberRowsToTake * 2)
            sinkEntityManager <- icebergUtil.getSinkEntityManager
            _                 <- sinkEntityManager.createTable(IcebergCreateTableRequest(targetTableName, schema, true))
            sinkPropertyManager <- icebergUtil.getSinkTablePropertyManager
            source <- ZIO.succeed(
              PushStreamingSource(
                sourceTableName = sourceTableName,
                targetTableName = targetTableName,
                primaryKeyFieldName = primaryKeyField,
                primaryKeyValue = primaryKeyValue,
                watermarkFieldName = watermarkField,
                dynamodbClient = client,
                sinkPropertyManager = sinkPropertyManager
              )
            )
            provider <- ZIO.succeed(
              PushStreamSourceDataProvider(
                source,
                sinkPropertyManager,
                defaultSinkSettings,
                TestThroughputShaperBuilder.default(sinkPropertyManager, defaultSinkSettings),
                TestSourceBufferingSettings
              )
            )
            // seed the sink table watermark so the streaming provider has a starting point
            _ <- icebergUtil.prepareWatermark(targetTableName, PushStreamWatermark.epoch, Some(schema))
            streamingDataProvider <- ZIO.succeed(
              PushStreamStreamingDataProvider(
                provider,
                defaultStreamMode.changeCapture,
                DeclaredMetrics()
              )
            )
            lifetimeService <- ZIO.succeed(TestStreamLifetimeService(numberRowsToTake))
            rows <- streamingDataProvider.stream
              .flatMap(_._1)
              .rechunk(1)
              .takeUntil(_ => lifetimeService.cancelled)
              .runCollect
          yield assertTrue(rows.size == numberRowsToTake)
        }
      yield result
    }
  } @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
