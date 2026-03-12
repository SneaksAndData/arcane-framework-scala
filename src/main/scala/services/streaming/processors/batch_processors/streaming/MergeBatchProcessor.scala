package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors.streaming

import logging.ZIOLogAnnotations.*
import models.app.PluginStreamContext
import models.batches.{MergeableBatch, StagedVersionedBatch}
import models.settings.*
import models.settings.TableNaming.*
import models.settings.backfill.BackfillBehavior.Overwrite
import models.settings.sink.SinkSettings
import services.base.MergeServiceClient
import services.iceberg.base.*
import services.iceberg.given_Conversion_Schema_ArcaneSchema
import services.merging.JdbcTableManager
import services.metrics.DeclaredMetrics
import services.metrics.DeclaredMetrics.*
import services.streaming.base.StagedBatchProcessor

import zio.stream.ZPipeline
import zio.{Task, ZIO, ZLayer}

/** Processor that merges data into a target table.
  */
class MergeBatchProcessor(
    mergeServiceClient: MergeServiceClient,
    tableManager: JdbcTableManager,
    sinkEntityManager: SinkEntityManager,
    sinkPropertyManager: SinkPropertyManager,
    stagingEntityManager: StagingEntityManager,
    stagingPropertyManager: StagingPropertyManager,
    targetTableSettings: SinkSettings,
    declaredMetrics: DeclaredMetrics,
    schemaMigrationEnabled: Boolean,
    isTargetInStaging: Boolean
) extends StagedBatchProcessor:

  private def alignSchemas(
      batch: StagedVersionedBatch & MergeableBatch,
      propertyManager: TablePropertyManager,
      entityManager: CatalogEntityManager
  ): Task[Unit] = for
    targetSchema <- propertyManager.getTableSchema(batch.targetTableName.parts.name)
    _            <- entityManager.migrateSchema(targetSchema, batch.schema, batch.targetTableName.parts.name)
  yield ()

  /** Processes the incoming data.
    *
    * @return
    *   ZPipeline (stream source for the stream graph).
    */
  override def process: ZPipeline[Any, Throwable, BatchType, BatchType] =
    ZPipeline.mapZIO(batchesSet =>
      (for
        _ <- zlog(
          "Applying batch set with index %s",
          Seq(getAnnotation("processor", "MergeBatchProcessor")),
          batchesSet.batchIndex.toString
        )
        _ <- ZIO.foreach(batchesSet.groupedBySchema)(batch =>
          ZIO.when(!batch.isEmpty && schemaMigrationEnabled) {
            for
              // for streams, we migrate sink table
              _ <- ZIO.unless(isTargetInStaging)(alignSchemas(batch, sinkPropertyManager, sinkEntityManager))
              // for backfills, we migrate staging table
              _ <- ZIO.when(isTargetInStaging)(alignSchemas(batch, stagingPropertyManager, stagingEntityManager))
            yield ()
          }
        )
        _ <- ZIO
          .foreach(batchesSet.groupedBySchema)(batch => ZIO.unless(batch.isEmpty)(mergeServiceClient.applyBatch(batch)))

        _ <- ZIO.unless(batchesSet.groupedBySchema.isEmpty || batchesSet.groupedBySchema.head.isEmpty) {
          for
            _ <- tableManager.optimizeTable(
              batchesSet.getOptimizationRequest(targetTableSettings.maintenanceSettings.targetOptimizeSettings)
            )
            _ <- tableManager.expireSnapshots(
              batchesSet.getSnapshotExpirationRequest(
                targetTableSettings.maintenanceSettings.targetSnapshotExpirationSettings
              )
            )
            _ <- tableManager.expireOrphanFiles(
              batchesSet.getOrphanFileExpirationRequest(
                targetTableSettings.maintenanceSettings.targetOrphanFilesExpirationSettings
              )
            )
            _ <- tableManager.analyzeTable(
              batchesSet.getAnalyzeRequest(
                targetTableSettings.maintenanceSettings.targetAnalyzeSettings
              )
            )
          yield ()
        }
      yield batchesSet).gaugeDuration(declaredMetrics.batchMergeStageDuration)
    )

object MergeBatchProcessor:

  /** Factory method to create MergeProcessor
    *
    * @param mergeServiceClient
    *   The JDBC consumer.
    * @param targetTableSettings
    *   The target table settings.
    * @return
    *   The initialized MergeProcessor instance
    */
  def apply(
      mergeServiceClient: MergeServiceClient,
      sinkEntityManager: SinkEntityManager,
      sinkPropertyManager: SinkPropertyManager,
      stagingEntityManager: StagingEntityManager,
      stagingPropertyManager: StagingPropertyManager,
      tableManager: JdbcTableManager,
      targetTableSettings: SinkSettings,
      declaredMetrics: DeclaredMetrics,
      schemaMigrationEnabled: Boolean,
      isBackfilling: Boolean
  ): MergeBatchProcessor =
    new MergeBatchProcessor(
      mergeServiceClient,
      tableManager,
      sinkEntityManager,
      sinkPropertyManager,
      stagingEntityManager,
      stagingPropertyManager,
      targetTableSettings,
      declaredMetrics,
      schemaMigrationEnabled,
      isBackfilling
    )

  /** The required environment for the MergeBatchProcessor.
    */
  type Environment = MergeServiceClient & PluginStreamContext & SinkEntityManager & SinkPropertyManager &
    StagingEntityManager & StagingPropertyManager & JdbcTableManager & DeclaredMetrics

  /** The ZLayer that creates the MergeProcessor.
    */
  val layer: ZLayer[Environment, Nothing, MergeBatchProcessor] =
    ZLayer {
      for
        context                <- ZIO.service[PluginStreamContext]
        jdbcConsumer           <- ZIO.service[MergeServiceClient]
        sinkEntityManager      <- ZIO.service[SinkEntityManager]
        sinkPropertyManager    <- ZIO.service[SinkPropertyManager]
        stagingEntityManager   <- ZIO.service[StagingEntityManager]
        stagingPropertyManager <- ZIO.service[StagingPropertyManager]
        tableManager           <- ZIO.service[JdbcTableManager]
        declaredMetrics        <- ZIO.service[DeclaredMetrics]
      yield MergeBatchProcessor(
        jdbcConsumer,
        sinkEntityManager,
        sinkPropertyManager,
        stagingEntityManager,
        stagingPropertyManager,
        tableManager,
        context.sink,
        declaredMetrics,
        !context.staging.table.isUnifiedSchema,
        context.isBackfilling && context.streamMode.backfill.backfillBehavior == Overwrite
      )
    }
