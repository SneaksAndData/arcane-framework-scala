package com.sneaksanddata.arcane.framework
package tests.pullstream

import models.ddl.CreateTableRequest as IcebergCreateTableRequest
import models.schemas.ArcaneType.StringType
import models.schemas.{ArcaneSchema, DataRow, Field, IndexedField, IndexedMergeKeyField, MergeKeyField}
import services.filters.FieldsFilteringService
import services.iceberg.base.SinkPropertyManager
import services.iceberg.given_Conversion_ArcaneSchema_Schema
import services.iceberg.{IcebergCatalogFactory, IcebergS3CatalogWriter, IcebergStagingEntityManager}
import services.merging.JdbcMergeServiceClient
import services.merging.cleanup.CatalogDisposeServiceClient
import services.metrics.DeclaredMetrics
import services.naming.DefaultNameGenerator
import services.pullstream.versioning.PullStreamWatermark
import services.pullstream.{PullStreamStagedBatchFactory, PullStreamingSource}
import services.streaming.base.{StreamDataProvider, StructuredZStream}
import services.streaming.graph.DefaultStreamingGraphBuilder
import services.streaming.processors.batch_processors.maintenance.TargetMaintenanceProcessor
import services.streaming.processors.batch_processors.streaming.{
  DisposeBatchProcessor,
  MergeBatchProcessor,
  WatermarkProcessor
}
import services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}
import tests.pullstream.util.PullStreamTestServices
import tests.shared.*
import tests.shared.IcebergCatalogInfo.defaultIcebergStagingSettings
import tests.shared.TestTrinoConnection.{getFieldValueInTarget, getRowsInTarget, newTrinoConnection}

import zio.stream.ZStream
import zio.test.TestAspect.timeout
import zio.test.{Spec, TestAspect, TestEnvironment, ZIOSpecDefault, assertTrue}
import zio.{Ref, Scope, ZIO, ZLayer}

import java.time.{Instant, OffsetDateTime, ZoneOffset}

/** Emits whatever the pull stream source produces for a given watermark, so the graph under test is driven by real
  * DynamoDB items rather than synthetic rows.
  */
final class PullStreamTestDataProvider(source: PullStreamingSource, from: PullStreamWatermark)
    extends StreamDataProvider:
  override def stream: ZStream[Any, Throwable, StructuredZStream] = source.getChanges(from)

/** End-to-end coverage for the watermark column, exercising the real docker-compose stack: DynamoDB local as the
  * source, Lakekeeper + MinIO as the Iceberg catalog and storage, and Trino as the merge engine.
  *
  * The payload deliberately omits the watermark field: its value exists only as an attribute of the DynamoDB item, so
  * finding it in the target table proves it survived decoding, staging and the MERGE.
  */
object PullStreamEndToEndTests extends ZIOSpecDefault:

  private val targetTableName     = "pull_stream_watermark_e2e"
  private val targetTableFullName = s"iceberg.test.$targetTableName"
  private val icebergUtil         = IcebergUtil(TestDynamicSinkSettings(targetTableName).icebergCatalog)

  /** The sink stores the watermark under a lowercase name while the DynamoDB attribute is `timestampUTC`, mirroring a
    * target table created through an engine that folds unquoted identifiers.
    */
  private val watermarkColumn = "timestamputc"

  /** The decoded production payload, plus the two columns the framework synthesizes from the envelope. */
  private val targetSchema: ArcaneSchema = ArcaneSchema(
    PullStreamTestServices.productionPayloadSchema
      ++ Seq(Field(watermarkColumn, StringType), MergeKeyField)
  )

  private val writerLayer: ZLayer[Any, Throwable, IcebergS3CatalogWriter] = ZLayer.scoped {
    for
      factory <- IcebergCatalogFactory.live(defaultIcebergStagingSettings)
      entityManager = IcebergStagingEntityManager(defaultIcebergStagingSettings, factory)
    yield IcebergS3CatalogWriter(entityManager, TestStagingSettings())
  }

  private val mergeServiceClient =
    new JdbcMergeServiceClient(TestJdbcMergeServiceClientSettings, "iceberg", "test", DeclaredMetrics(), false)

  private def buildGraph(source: PullStreamingSource, from: PullStreamWatermark) =
    for
      writer               <- ZIO.service[IcebergS3CatalogWriter]
      sinkPropertyManager  <- ZIO.service[SinkPropertyManager]
      stagingEntityManager <- ZIO.service[services.iceberg.base.StagingEntityManager]
      schemaMigration <- ZIO.service[services.streaming.processors.batch_processors.streaming.SchemaMigrationProcessor]
      counter         <- Ref.make(0L)
      nameGenerator = DefaultNameGenerator(
        sinkSettings = TestDynamicSinkSettings(targetTableName),
        backfillId = "",
        streamId = "pull-stream-e2e"
      )
    yield DefaultStreamingGraphBuilder(
      streamDataProvider = PullStreamTestDataProvider(source, from),
      fieldFilteringProcessor = FieldFilteringTransformer(FieldsFilteringService(TestFieldSelectionRuleSettings)),
      stagingProcessor = StagingProcessor(
        targetTableFullName = targetTableFullName,
        icebergCatalogSettings = defaultIcebergStagingSettings,
        catalogWriter = writer,
        batchFactory = PullStreamStagedBatchFactory(source.versionFieldName),
        declaredMetrics = DeclaredMetrics(),
        nameGenerator = nameGenerator
      ),
      mergeProcessor = MergeBatchProcessor(mergeServiceClient, DeclaredMetrics()),
      disposeBatchProcessor = DisposeBatchProcessor(CatalogDisposeServiceClient(stagingEntityManager), false),
      watermarkProcessor = WatermarkProcessor(sinkPropertyManager, targetTableName, DeclaredMetrics()),
      schemaMigrationProcessor = schemaMigration,
      targetMaintenanceProcessor = TargetMaintenanceProcessor(
        counterRef = counter,
        options = TestJdbcMergeServiceClientSettings,
        maintenanceSettings = TestTableMaintenanceSettings,
        defaultCatalogName = "iceberg",
        defaultSchemaName = "test",
        declaredMetrics = DeclaredMetrics(),
        isBackfilling = false
      )
    )

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("PullStreamEndToEndTests")(
    test("writes the DynamoDB watermark attribute into the Iceberg target table") {
      val totalItems = 3
      val startAt    = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC).plusHours(1)
      val readFrom   = PullStreamWatermark(OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC))

      for
        sourceTableName <- PullStreamTestServices.genSourceTableName
        client          <- PullStreamTestServices.getClient
        result <- PullStreamTestServices.withSourceTable(sourceTableName, client) {
          for
            sinkEntityManager <- ZIO.service[services.iceberg.base.SinkEntityManager]
            _ <- sinkEntityManager.createTable(IcebergCreateTableRequest(targetTableName, targetSchema, true))
            sinkPropertyManager <- ZIO.service[SinkPropertyManager]
            source = PullStreamingSource(
              settings = PullStreamTestServices.pullStreamSettings(sourceTableName),
              dynamodbClient = client,
              sinkPropertyManager = sinkPropertyManager,
              targetTableFullName = targetTableFullName,
              pageSize = None
            )
            _ <- PullStreamTestServices.insertMany(
              client,
              sourceTableName,
              count = totalItems,
              startAt = startAt,
              payload = PullStreamTestServices.productionPayload
            )
            graph      <- buildGraph(source, readFrom)
            _          <- graph.produce().runCollect
            connection <- newTrinoConnection
            rowCount   <- getRowsInTarget(connection, targetTableFullName)
            firstItemKey = PullStreamTestServices.defaultId(0)
            watermark <- getFieldValueInTarget(
              connection,
              targetTableFullName,
              watermarkColumn,
              MergeKeyField.name,
              firstItemKey
            )
            nestedPayload <- getFieldValueInTarget(
              connection,
              targetTableFullName,
              "payload",
              MergeKeyField.name,
              firstItemKey
            )
            businessId <- getFieldValueInTarget(
              connection,
              targetTableFullName,
              "id",
              MergeKeyField.name,
              firstItemKey
            )
          yield
            // one target row per DynamoDB item: a missing or constant merge key would have collapsed them
            assertTrue(rowCount == totalItems)
            // the watermark is absent from the payload, so it can only have come from the item attribute
              && assertTrue(watermark == startAt.toString)
              // the row is addressable by the envelope's `id`, proving that supplied the merge key, while `id` inside
              // the decoded payload keeps its own distinct business value
              && assertTrue(businessId == "evt_001")
              // the nested object is stored verbatim rather than being flattened or dropped
              && assertTrue(nestedPayload == PullStreamTestServices.productionNestedPayload)
        }
      yield result
    }
  ).provide(
    writerLayer,
    icebergUtil.getSinkEntityManagerLayer,
    icebergUtil.getStagingEntityManagerLayer,
    icebergUtil.getSinkTablePropertyManagerLayer,
    VoidSchemaMigrationProcessor.layer
  ) @@ timeout(zio.Duration.fromSeconds(120)) @@ TestAspect.withLiveClock @@ TestAspect.withLiveRandom
