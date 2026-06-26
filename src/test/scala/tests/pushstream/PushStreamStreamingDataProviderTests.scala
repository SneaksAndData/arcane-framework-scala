package com.sneaksanddata.arcane.framework
package tests.pushstream

import models.ddl.CreateTableRequest as IcebergCreateTableRequest
import models.schemas.{ArcaneSchema, ArcaneType, Field}
import models.settings.backfill.BackfillBehavior.Overwrite
import models.settings.backfill.{BackfillBehavior, BackfillSettings}
import models.settings.sources.pushstream.PushStreamSourceSettings
import models.settings.streaming.{ChangeCaptureSettings, StreamModeSettings}
import services.iceberg.SchemaConversions.toIcebergSchema
import services.metrics.DeclaredMetrics
import services.pushstream.{PushStreamSourceDataProvider, PushStreamStreamingDataProvider, PushStreamingSource}
import services.pushstream.versioning.PushStreamWatermark
import tests.shared.*

import com.sneaksanddata.arcane.framework.services.iceberg.interop.MissingFieldException
import org.apache.avro.AvroTypeException
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, PutItemRequest, PutItemResponse}
import zio.test.TestAspect.timeout
import zio.test.{Spec, TestAspect, TestEnvironment, ZIOSpecDefault, assertTrue}
import zio.{Random, Scope, Task, ZIO}

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

  private val primaryKeyField    = "producer"
  private val primaryKeyValue    = "producer1"
  private val watermarkField     = "timestampUTC"

  /** Per-test settings bound to a freshly generated source/target table name pair. */
  private def pushStreamSettings(srcTable: String, tgtTable: String): PushStreamSourceSettings =
    new PushStreamSourceSettings:
      override val sourceTableName: String     = srcTable
      override val targetTableName: String     = tgtTable
      override val primaryKeyFieldName: String = primaryKeyField
      override val primaryKeyValue: String     = PushStreamStreamingDataProviderTests.primaryKeyValue
      override val watermarkFieldName: String  = watermarkField
      override val region: String              = "us-east-1"
      override val tableName: String           = srcTable
      override val endpoint: Option[String]    = None

  // Random, collision-free DynamoDB table name.
  private val genSourceTableName: Task[String] = {
    Random.RandomLive.nextUUID.map(uuid => s"test_${uuid.toString.replace("-", "")}")

  }

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
        .s(s"""{"userId": "$userId","level": "$level"}""")
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

  private def insertMalformedPayload(client: DynamoDbClient, tableName: String): Task[PutItemResponse] =
    val timestamp = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC).plusHours(1)
    val item = Map(
      primaryKeyField -> AttributeValue.builder().s(primaryKeyValue).build(),
      watermarkField  -> AttributeValue.builder().s(timestamp.toString).build(),
      // not valid JSON — leading apostrophe trips Jackson before schema validation runs
      "payload"  -> AttributeValue.builder().s("""{"not":"valid json", "level": 1}""").build(),
      "schemaId" -> AttributeValue.builder().n("1").build()
    ).asJava
    ZIO.attemptBlocking(
      client.putItem(PutItemRequest.builder().tableName(tableName).item(item).build())
    )

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("PushStreamStreamingDataProviderTests")(
    test("returns correct number of rows while streaming") {
      val numberRowsToTake = 5
      for
        sourceTableName <- genSourceTableName
        targetTableName = s"demo.test.$sourceTableName"
        defaultSinkSettings = TestDynamicSinkSettings(targetTableName)
        icebergUtil         = IcebergUtil(defaultSinkSettings.icebergCatalog)
        client <- PushStreamTestServices.getClient
        result <- ZIO.acquireReleaseWith(
          PushStreamTestServices.createTable(sourceTableName, client)
        )(_ => PushStreamTestServices.deleteTable(client, sourceTableName).orDie) { _ =>
          for
            // seed source DDB table with more than `numberRowsToTake` rows
            _                 <- insertRows(client, sourceTableName, numberRowsToTake * 2)
            sinkEntityManager <- icebergUtil.getSinkEntityManager
            _                 <- sinkEntityManager.createTable(IcebergCreateTableRequest(sourceTableName, schema, true))
            sinkPropertyManager <- icebergUtil.getSinkTablePropertyManager
            source <- ZIO.succeed(
              PushStreamingSource(
                settings = pushStreamSettings(sourceTableName, targetTableName),
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
            _ <- icebergUtil.prepareWatermark(sourceTableName, PushStreamWatermark.epoch, Some(schema))
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
    },
    test("fails with MissingFieldException when payload does not match the schema") {
      for
        sourceTableName <- genSourceTableName
        targetTableName = s"demo.test.$sourceTableName"
        defaultSinkSettings = TestDynamicSinkSettings(targetTableName)
        icebergUtil         = IcebergUtil(defaultSinkSettings.icebergCatalog)
        client <- PushStreamTestServices.getClient
        result <- ZIO.acquireReleaseWith(
          PushStreamTestServices.createTable(sourceTableName, client)
        )(_ => PushStreamTestServices.deleteTable(client, sourceTableName).orDie) { _ =>
          for
            _                 <- insertMalformedPayload(client, sourceTableName)
            sinkEntityManager <- icebergUtil.getSinkEntityManager
            _                 <- sinkEntityManager.createTable(IcebergCreateTableRequest(sourceTableName, schema, true))
            sinkPropertyManager <- icebergUtil.getSinkTablePropertyManager
            source <- ZIO.succeed(
              PushStreamingSource(
                settings = pushStreamSettings(sourceTableName, targetTableName),
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
            _ <- icebergUtil.prepareWatermark(sourceTableName, PushStreamWatermark.epoch, Some(schema))
            streamingDataProvider <- ZIO.succeed(
              PushStreamStreamingDataProvider(
                provider,
                defaultStreamMode.changeCapture,
                DeclaredMetrics()
              )
            )
            outcome <- streamingDataProvider.stream
              .flatMap(_._1)
              .runCollect
              .sandbox
              .either
          yield assertTrue(
            outcome.left.exists(c =>
              c.defects.exists(_.isInstanceOf[MissingFieldException]) ||
                c.failures.exists(_.isInstanceOf[MissingFieldException])
            )
          )
        }
      yield result
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock @@ TestAspect.withLiveRandom
