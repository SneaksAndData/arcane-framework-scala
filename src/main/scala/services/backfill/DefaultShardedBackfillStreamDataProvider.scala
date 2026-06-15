package com.sneaksanddata.arcane.framework
package services.backfill

import logging.ZIOLogAnnotations.zlog
import models.settings.backfill.BackfillSettings
import models.sharding.BootstrappedShard
import services.backfill.base.{BackfillSourceDataProvider, ShardedBackfillStreamDataProvider}
import services.metrics.DeclaredMetrics
import services.streaming.base.{JsonWatermark, SourceWatermark}

import upickle.ReadWriter
import zio.stream.ZStream
import zio.{Task, ZIO}

class DefaultShardedBackfillStreamDataProvider[WatermarkType <: SourceWatermark[String] & JsonWatermark](
    dataProvider: BackfillSourceDataProvider[WatermarkType],
    backfillSettings: BackfillSettings,
    stateManager: DefaultBackfillStateManager,
    declaredMetrics: DeclaredMetrics
)(implicit rw: ReadWriter[WatermarkType])
    extends ShardedBackfillStreamDataProvider:

  def backfillStream: Task[(stream: ZStream[Any, Throwable, BootstrappedShard], watermark: JsonWatermark)] =
    stateManager.readState
      .flatMap {
        case Some(state) =>
          for
            existingWatermark <- ZIO.attempt(upickle.read(state.backfillEnd))
            _ <- zlog(
              "Resuming backfill with watermark '%s', total shards %s",
              state.watermarkValue,
              state.shardSources.size.toString
            )
          yield (
            stream = dataProvider.requestBackfill(upickle.read(state.backfillEnd), Some(state.shardSources)),
            watermark = upickle.read(state.backfillEnd)
          )

        case None =>
          dataProvider.getSnapshotVersion
            .map(watermark => (stream = dataProvider.requestBackfill(watermark, None), watermark = watermark))
      }
