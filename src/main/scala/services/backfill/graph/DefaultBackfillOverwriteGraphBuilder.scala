package com.sneaksanddata.arcane.framework
package services.backfill.graph

import logging.ZIOLogAnnotations.{zlog, zlogStream}
import models.sharding.{CompletedShard, CompletionShard, StagedShard}
import services.backfill.base.{BackfillStateManager, ShardedBackfillStreamDataProvider, ShardFactory}
import services.backfill.processors.{BackfillCompletionProcessor, ShardStagingProcessor}
import services.base.MergeServiceClient
import services.streaming.processors.transformers.FieldFilteringTransformer

import zio.stream.{ZPipeline, ZSink, ZStream}
import zio.{ZIO, ZLayer}

/** Generates a stream graph for backfill OVERWRITE mode.
  */
class DefaultBackfillOverwriteGraphBuilder(
    streamDataProvider: ShardedBackfillStreamDataProvider,
    shardStageProcessor: ShardStagingProcessor,
    mergeServiceClient: MergeServiceClient,
    stateManager: BackfillStateManager,
    shardFactory: ShardFactory,
    fieldFilteringProcessor: FieldFilteringTransformer,
    backfillCompletionProcessor: BackfillCompletionProcessor
) extends BackfillStreamingGraphBuilder:

  private val backfillParallelism     = Runtime.getRuntime.availableProcessors()
  private def aggregateStagedShards   = ZPipeline.fromSink(ZSink.last[StagedShard])
  private def aggregateCombinedShards = ZPipeline.fromSink(ZSink.last[CompletionShard])

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

      stream
        .flatMapPar(backfillParallelism / 2, backfillParallelism) { shard =>
          ZStream
            .fromZIO(stateManager.isStaged(shard))
            .flatMap { isStaged =>
              if isStaged then
                zlogStream("Shard %s has been staged previously, skipping", shard.shardId) *> ZStream.fromZIO(
                  shardFactory.createStagedShard(shard)
                )
              else
                ZStream
                  .fromZIO(stateManager.prepareShardStage(shard, shard.shardStream._2))
                  .flatMap(_ =>
                    shard.shardStream._1
                      .via(fieldFilteringProcessor.process)
                      .via(shardStageProcessor.process(shard, shard.shardStream._2))
                      .via(aggregateStagedShards)
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
                  "Shard %s has been added to the combined backfill table already, skipping",
                  completion.shardId
                ) *> ZStream.succeed(completion)
              case None =>
                ZStream.succeed(staged).mapZIO { staged =>
                  for
                    _ <- zlog("Shard %s fully commited, ready for combine", staged.shardId)
                    _ <- mergeServiceClient.commitShard(staged)
                    _ <- zlog(
                      "Shard %s data has been successfully commited to the combined backfill table",
                      staged.shardId
                    )
                    completionShard <- shardFactory.createCompletionShard(staged, watermark.toJson)
                  yield completionShard
                }
            }
            .mapZIO(shard => stateManager.commitCombinedShard(shard))
        )
        .via(aggregateCombinedShards)
        .collect { case Some(completion) =>
          completion.copy(shardSourceEntityName = completion.combinedTableName)
        }
        .via(backfillCompletionProcessor.process)
    }

object DefaultBackfillOverwriteGraphBuilder:

  /** The environment required for the DefaultBackfillOverwriteGraphBuilder.
    */
  type Environment = ShardedBackfillStreamDataProvider & ShardStagingProcessor & MergeServiceClient &
    FieldFilteringTransformer & BackfillCompletionProcessor & BackfillStateManager & ShardFactory

  /** Creates a new DefaultBackfillOverwriteGraphBuilder.
    */
  def apply(
      streamDataProvider: ShardedBackfillStreamDataProvider,
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
        streamDataProvider         <- ZIO.service[ShardedBackfillStreamDataProvider]
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
