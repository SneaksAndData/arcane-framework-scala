package com.sneaksanddata.arcane.framework
package services.streaming.data_providers.backfill

import logging.ZIOLogAnnotations.{zlog, zlogStream}
import models.ArcaneSchema
import models.settings.{BackfillSettings, TablePropertiesSettings}
import services.app.base.StreamLifetimeService
import services.consumers.{MergeableBatch, StagedVersionedBatch}
import services.streaming.base.{BackfillStreamingOverwriteDataProvider, BackfillSubStream, HookManager, StreamingGraphBuilder}
import services.streaming.processors.transformers.StagingProcessor

import com.sneaksanddata.arcane.framework.services.streaming.graph_builders.GenericStreamingGraphBuilder
import org.apache.iceberg.Table
import zio.stream.ZPipeline
import zio.{Chunk, Task, ZIO, ZLayer}


/**
 * Provides the backfill data stream for the streaming process.
 * It is utilized when the backfill process begins with the `overwrite` behavior.
 * An important distinction between this and the GenericBackfillStreamingMergeDataProvider
 * is that this provider overrides the table used by the basic streamGraphBuilder,
 * replacing it with the intermediate backfill table.
 * Additionally, this data provider can generate a backfill batch as a result of the
 * backfill process, or it may produce nothing if the backfill was interrupted.
 * @param streamingGraphBuilder The streaming graph builder.
 * @param backfillTableSettings The backfill table settings.
 * @param lifetimeService The stream lifetime service.
 * @param baseHookManager The base hook manager.
 */
class GenericBackfillStreamingOverwriteDataProvider(streamingGraphBuilder: BackfillSubStream,
                                                    backfillTableSettings: BackfillSettings,
                                                    lifetimeService: StreamLifetimeService,
                                                    baseHookManager: HookManager,
                                                    backfillBatchFactory: BackfillOverwriteBatchFactory)
  extends BackfillStreamingOverwriteDataProvider:

  /**
   * @inheritdoc
   */
  def requestBackfill: Task[BatchType] =
    for
      _ <- zlog(s"Starting backfill process")
      _ <- streamingGraphBuilder
        .produce(BackfillHookManager(baseHookManager, backfillTableSettings))
        .via(streamLifetimeGuard)
        .runDrain
        .catchAllCause(e => zlog("Backfill process failed", e))
      _ <- zlog("Backfill process completed")
      
      backfillBatch <- if lifetimeService.cancelled then
        ZIO.unit
      else
        backfillBatchFactory.createBackfillBatch
    yield backfillBatch

  private def streamLifetimeGuard = ZPipeline.takeUntil(_ => lifetimeService.cancelled)


/**
 * The companion object for the GenericBackfillStreamingOverwriteDataProvider class.
 */
object GenericBackfillStreamingOverwriteDataProvider:

  /**
   * The environment required for the GenericBackfillStreamingOverwriteDataProvider.
   */
  type Environment = BackfillSubStream
    & BackfillSettings
    & StreamLifetimeService
    & BackfillOverwriteBatchFactory
    & HookManager

  /**
   * Creates a new GenericBackfillStreamingOverwriteDataProvider.
   * @param streamingGraphBuilder The streaming graph builder.
   * @param backfillTableSettings The backfill table settings.
   * @param lifetimeService The stream lifetime service.
   * @param baseHookManager The base hook manager.
   * @return The GenericBackfillStreamingOverwriteDataProvider instance.
   */
  def apply(streamingGraphBuilder: BackfillSubStream,
            backfillTableSettings: BackfillSettings,
            lifetimeService: StreamLifetimeService,
            baseHookManager: HookManager,
            backfillBatchFactory: BackfillOverwriteBatchFactory): GenericBackfillStreamingOverwriteDataProvider =
    new GenericBackfillStreamingOverwriteDataProvider(streamingGraphBuilder, backfillTableSettings, lifetimeService, baseHookManager, backfillBatchFactory)


  /**
   * The ZLayer for the GenericBackfillStreamingOverwriteDataProvider.
   */
  val layer: ZLayer[Environment, Nothing, BackfillStreamingOverwriteDataProvider] =
    ZLayer {
      for
        streamingGraphBuilder <- ZIO.service[BackfillSubStream]
        backfillTableSettings <- ZIO.service[BackfillSettings]
        lifetimeService <- ZIO.service[StreamLifetimeService]
        backfillBatchFactory <- ZIO.service[BackfillOverwriteBatchFactory]
        hookManager <- ZIO.service[HookManager]
      yield GenericBackfillStreamingOverwriteDataProvider(streamingGraphBuilder,
        backfillTableSettings,
        lifetimeService,
        hookManager,
        backfillBatchFactory)
    }

/**
 * The hook manager used for the backfill process.
 * This manager overrides the target table used by the base hook manager with the backfill table.
 *
 * @param base The base hook manager.
 * @param backfillTableSettings The backfill table settings.
 */
private class BackfillHookManager(base: HookManager, backfillTableSettings: BackfillSettings) extends HookManager:

  /**
   * @inheritdoc
   */
  def onStagingTablesComplete(staged: Iterable[StagedVersionedBatch & MergeableBatch], index: Long, others: Chunk[Any]): StagingProcessor#OutgoingElement =
    base.onStagingTablesComplete(staged, index, others)

  /**
   * @inheritdoc
   */
  def onBatchStaged(table: Table,
                    namespace: String,
                    warehouse: String,
                    batchSchema: ArcaneSchema,
                    targetName: String,
                    tablePropertiesSettings: TablePropertiesSettings): StagedVersionedBatch & MergeableBatch =
    base.onBatchStaged(table, namespace, warehouse, batchSchema, backfillTableSettings.backfillTableFullName, tablePropertiesSettings)


