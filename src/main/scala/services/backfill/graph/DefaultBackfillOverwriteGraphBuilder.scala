package com.sneaksanddata.arcane.framework
package services.backfill.graph

import logging.ZIOLogAnnotations.{zlog, zlogStream}
import models.sharding.{CompletedShard, StagedShard}
import services.backfill.base.{BackfillStateManager, BackfillStreamDataProvider, ShardFactory}
import services.backfill.processors.{BackfillCompletionProcessor, ShardCombineProcessor, ShardStagingProcessor}
import services.base.MergeServiceClient
import services.streaming.processors.transformers.FieldFilteringTransformer

import zio.stream.{ZPipeline, ZSink, ZStream}
import zio.{ZIO, ZLayer}

/** Provides the complete data stream for the streaming process including all the stages and services except the sink
  * and lifetime service.
  *
  * This graph builder is used for running a backfill process when backfill behavior is set to overwrite. The graph
  * builder works in the following way:
  *   1. Creates the backfilling stream data provider that manages the backfilling process and produces the mergeable
  *      batch targeting the intermediate table used as target for backfill stream.
  *   2. Applying the resulting batch to the target table using the SQL `CREATE OR REPLACE TABLE` statement.
  *   3. Since the table is replaced, the dispose batch processor is not needed and the graph builder.
  */
class DefaultBackfillOverwriteGraphBuilder(
    streamDataProvider: BackfillStreamDataProvider,
    shardStageProcessor: ShardStagingProcessor,
    mergeServiceClient: MergeServiceClient,
    stateManager: BackfillStateManager,
    shardFactory: ShardFactory,
    fieldFilteringProcessor: FieldFilteringTransformer,
    backfillCompletionProcessor: BackfillCompletionProcessor
) extends BackfillStreamingGraphBuilder:

  private val backfillParallelism = Runtime.getRuntime.availableProcessors() * 2
  private def aggregateShards     = ZPipeline.fromSink(ZSink.last[StagedShard])

  /** @inheritdoc
    */
  override type ProcessedBatch = CompletedShard

  /** @inheritdoc
    */
  override def produce(): ZStream[Any, Throwable, ProcessedBatch] = ZStream
    .fromZIO(
      zlog(
        "Backfill with use shard parallelism of %s",
        backfillParallelism.toString
      ) *> streamDataProvider.backfillStream
    )
    .flatMap { case (stream, watermark) =>
      val combineProcessor = ShardCombineProcessor(mergeServiceClient, shardFactory, watermark)

      stream
        .flatMapPar(backfillParallelism, 1) { shard =>
          ZStream
            .fromZIO(stateManager.isStaged(shard))
            .flatMap { isStaged =>
              if isStaged then
                zlogStream("Shard %s has been staged previously, skipping", shard.shardId) *> ZStream.succeed(
                  shardFactory.createStagedShard(shard)
                )
              else
                ZStream
                  .fromZIO(stateManager.prepareShardStage(shard, shard.shardStream._2))
                  .flatMap(_ =>
                    shard.shardStream._1
                      .via(fieldFilteringProcessor.process)
                      .via(shardStageProcessor.process(shard, shard.shardStream._2))
                      .via(aggregateShards)
                      .collect { case Some(staged) =>
                        staged
                      }
                      .mapZIO(stateManager.commitStagedShard)
                  )
            }
        }
        .flatMap(staged =>
          ZStream
            .fromZIO(stateManager.isCombined(staged))
            .flatMap {
              case Some(completion) =>
                zlogStream(
                  "Shard %s has been added to the combined table already, skipping",
                  completion.shardId
                ) *> ZStream.succeed(completion)
              case None => ZStream.succeed(staged).via(combineProcessor.process)
            }
            .mapZIO(shard => stateManager.commitCombinedShard(shard))
        )
        .via(backfillCompletionProcessor.process)
    }

object DefaultBackfillOverwriteGraphBuilder:

  /** The environment required for the DefaultBackfillOverwriteGraphBuilder.
    */
  type Environment = BackfillStreamDataProvider & ShardStagingProcessor & MergeServiceClient &
    FieldFilteringTransformer & BackfillCompletionProcessor & BackfillStateManager & ShardFactory

  /** Creates a new DefaultBackfillOverwriteGraphBuilder.
    */
  def apply(
      streamDataProvider: BackfillStreamDataProvider,
      shardStageProcessor: ShardStagingProcessor,
      mergeServiceClient: MergeServiceClient,
      fieldFilteringProcessor: FieldFilteringTransformer,
      backfillCompletionProcessor: BackfillCompletionProcessor,
      stateManager: BackfillStateManager,
      shardFactory: ShardFactory
  ): DefaultBackfillOverwriteGraphBuilder =
    new DefaultBackfillOverwriteGraphBuilder(
      streamDataProvider,
      shardStageProcessor,
      mergeServiceClient,
      stateManager,
      shardFactory,
      fieldFilteringProcessor,
      backfillCompletionProcessor
    )

  /** The ZLayer for the DefaultBackfillOverwriteGraphBuilder.
    */
  val layer: ZLayer[Environment, Nothing, DefaultBackfillOverwriteGraphBuilder] =
    ZLayer {
      for
        streamDataProvider         <- ZIO.service[BackfillStreamDataProvider]
        shardStageProcessor        <- ZIO.service[ShardStagingProcessor]
        mergeServiceClient         <- ZIO.service[MergeServiceClient]
        fieldFilteringProcessor    <- ZIO.service[FieldFilteringTransformer]
        backfillWatermarkProcessor <- ZIO.service[BackfillCompletionProcessor]
        stateManager               <- ZIO.service[BackfillStateManager]
        shardFactory               <- ZIO.service[ShardFactory]
      yield DefaultBackfillOverwriteGraphBuilder(
        streamDataProvider,
        shardStageProcessor,
        mergeServiceClient,
        fieldFilteringProcessor,
        backfillWatermarkProcessor,
        stateManager,
        shardFactory
      )
    }
