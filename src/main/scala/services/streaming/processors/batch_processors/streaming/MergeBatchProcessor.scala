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
import services.metrics.DeclaredMetrics
import services.metrics.DeclaredMetrics.*
import services.streaming.base.StagedBatchProcessor

import com.sneaksanddata.arcane.framework.models.schemas.ArcaneSchema
import zio.stream.ZPipeline
import zio.{Task, ZIO, ZLayer}

/** Processor that merges data into a target table.
  */
class MergeBatchProcessor(
    mergeServiceClient: MergeServiceClient,
    sinkEntityManager: SinkEntityManager,
    sinkPropertyManager: SinkPropertyManager,
    stagingEntityManager: StagingEntityManager,
    stagingPropertyManager: StagingPropertyManager,
    targetTableSettings: SinkSettings,
    declaredMetrics: DeclaredMetrics,
    schemaMigrationEnabled: Boolean,
    isTargetInStaging: Boolean
) extends StagedBatchProcessor:

  /** Processes the incoming data.
    *
    * @return
    *   ZPipeline (stream source for the stream graph).
    */
  override def process: ZPipeline[Any, Throwable, BatchType, BatchType] =
    ZPipeline.mapZIO(batch =>
      (for
        _ <- zlog(
          "Applying batch %s",
          Seq(getAnnotation("processor", "MergeBatchProcessor")),
          batch.name
        )
        _ <- ZIO.unless(batch.isEmpty)(mergeServiceClient.applyBatch(batch))
      yield batch).gaugeDuration(declaredMetrics.batchMergeStageDuration)
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
      targetTableSettings: SinkSettings,
      declaredMetrics: DeclaredMetrics,
      schemaMigrationEnabled: Boolean,
      isBackfilling: Boolean
  ): MergeBatchProcessor =
    new MergeBatchProcessor(
      mergeServiceClient,
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
    StagingEntityManager & StagingPropertyManager & DeclaredMetrics

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
        declaredMetrics        <- ZIO.service[DeclaredMetrics]
        isBackfilling          <- context.isBackfilling.orElseSucceed(false)
      yield MergeBatchProcessor(
        jdbcConsumer,
        sinkEntityManager,
        sinkPropertyManager,
        stagingEntityManager,
        stagingPropertyManager,
        context.sink,
        declaredMetrics,
        !context.staging.table.isUnifiedSchema,
        isBackfilling && context.streamMode.backfill.backfillBehavior == Overwrite
      )
    }
