package com.sneaksanddata.arcane.framework
package services.backfill.graph

import services.backfill.BackfillStreamDataProvider
import services.streaming.base.{JsonWatermark, SourceWatermark, StreamDataProvider}
import services.streaming.processors.batch_processors.backfill.{BackfillOverwriteBatchProcessor, BackfillOverwriteWatermarkProcessor}

import com.sneaksanddata.arcane.framework.models.schemas.{ArcaneSchema, JsonWatermarkRow}
import com.sneaksanddata.arcane.framework.models.sharding.{StagedShard, WatermarkShard}
import com.sneaksanddata.arcane.framework.services.backfill.processors.{ShardProcessor, StagedShardBatch, WatermarkShardBatch}
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.FieldFilteringTransformer
import zio.stream.ZStream
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
class GenericBackfillOverwriteGraphBuilder(
                                            streamDataProvider: BackfillStreamDataProvider,
                                            applyBatchProcessor: BackfillOverwriteBatchProcessor,
                                            watermarkProcessor: BackfillOverwriteWatermarkProcessor,
                                            fieldFilteringProcessor: FieldFilteringTransformer,
                                            shardProcessor: ShardProcessor
) extends BackfillStreamingGraphBuilder:

  private val backfillParallelism = Runtime.getRuntime.availableProcessors() * 2

  /** @inheritdoc
    */
  override type ProcessedBatch = BackfillOverwriteBatchProcessor#BatchType

  /** @inheritdoc
    */
  override def produce(): ZStream[Any, Throwable, ProcessedBatch] =
    streamDataProvider.backfillStream
      .flatMapPar(backfillParallelism, 1){
        case shard: StagedShard => shard.shardStream._1
          .via(fieldFilteringProcessor.process)
          .via(shardProcessor.process(shard.shardStream._2))
        // TODO: due to parallelism this will cause watermark to be applied out of order
        // it should be instead propagated down and processed during shard merge
        case shard: WatermarkShard[SourceWatermark[String] & JsonWatermark] => ZStream.succeed(WatermarkShardBatch(shard.watermark.version))
        .via(watermarkProcessor.process)
          .map(_ => StagedShardBatch("", "", ArcaneSchema.empty()))
      }
//.via(applyBatchProcessor.process)
//.via(watermarkProcessor.process)
//      
//
//object GenericBackfillOverwriteGraphBuilder:
//
//  /** The environment required for the GenericBackfillGraphBuilder.
//    */
//  type Environment = BackfillStreamDataProvider & BackfillOverwriteBatchProcessor &
//    BackfillOverwriteWatermarkProcessor
//
//  /** Creates a new GenericBackfillGraphBuilder.
//    */
//  def apply(
//             streamDataProvider: BackfillStreamDataProvider,
//             mergeBatchProcessor: BackfillOverwriteBatchProcessor,
//             watermarkProcessor: BackfillOverwriteWatermarkProcessor
//  ): GenericBackfillOverwriteGraphBuilder =
//    new GenericBackfillOverwriteGraphBuilder(streamDataProvider, mergeBatchProcessor, watermarkProcessor)
//
//  /** The ZLayer for the GenericBackfillGraphBuilder.
//    */
//  val layer: ZLayer[Environment, Nothing, GenericBackfillOverwriteGraphBuilder] =
//    ZLayer {
//      for
//        streamDataProvider <- ZIO.service[BackfillStreamDataProvider]
//        mergeProcessor     <- ZIO.service[BackfillOverwriteBatchProcessor]
//        watermarkProcessor <- ZIO.service[BackfillOverwriteWatermarkProcessor]
//      yield GenericBackfillOverwriteGraphBuilder(streamDataProvider, mergeProcessor, watermarkProcessor)
//    }
