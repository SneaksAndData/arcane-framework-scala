package com.sneaksanddata.arcane.framework
package services.backfill

import logging.ZIOLogAnnotations.zlogStream
import models.settings.backfill.BackfillSettings
import models.sharding.{BootstrappedShard, SourceShard, StagedShard}
import services.metrics.DeclaredMetrics
import services.streaming.base.{JsonWatermark, SourceWatermark}

import zio.Task
import zio.stream.ZStream

class DefaultBackfillStreamDataProvider[WatermarkType <: SourceWatermark[String] & JsonWatermark](
                                                                                 dataProvider: BackfillSourceDataProvider[WatermarkType],
                                                                                 backfillSettings: BackfillSettings,
                                                                                 declaredMetrics: DeclaredMetrics
                                                                                 ) extends BackfillStreamDataProvider:
  

  def backfillStream: Task[(stream: ZStream[Any, Throwable, BootstrappedShard], watermark: JsonWatermark)] = dataProvider.getSnapshotVersion.map(watermark => (dataProvider.requestBackfill, watermark))

