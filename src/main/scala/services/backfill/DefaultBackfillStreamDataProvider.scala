package com.sneaksanddata.arcane.framework
package services.backfill

import logging.ZIOLogAnnotations.zlogStream
import models.settings.backfill.BackfillSettings
import models.sharding.{SourceShard, StagedShard}
import services.metrics.DeclaredMetrics
import services.streaming.base.SourceWatermark

import zio.Task
import zio.stream.ZStream

class DefaultBackfillStreamDataProvider[WatermarkType <: SourceWatermark[String]](
                                                                                 dataProvider: BackfillSourceDataProvider[WatermarkType],
                                                                                 backfillSettings: BackfillSettings,
                                                                                 declaredMetrics: DeclaredMetrics
                                                                                 ) extends BackfillStreamDataProvider:
  

  def backfillStream: ZStream[Any, Throwable, SourceShard] = ZStream.whenZIO(dataProvider.isEmpty.negate)(dataProvider.requestBackfill)

