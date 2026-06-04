package com.sneaksanddata.arcane.framework
package services.backfill

import models.settings.backfill.BackfillSettings
import services.backfill.base.{BackfillSourceDataProvider, BackfillStreamDataProvider}
import services.streaming.base.{ChangeCaptureDataProvider, JsonWatermark, SourceWatermark, StructuredZStream}

import zio.stream.ZStream

final class DefaultBackfillMergeStreamDataProvider[WatermarkType <: SourceWatermark[String] & JsonWatermark](
    dataProvider: ChangeCaptureDataProvider[WatermarkType],
    backfillSourceDataProvider: BackfillSourceDataProvider[WatermarkType],
    backfillSettings: BackfillSettings
) extends BackfillStreamDataProvider:
  override def stream: ZStream[Any, Throwable, StructuredZStream] = ZStream
    .fromZIO {
      for
        startFrom <- backfillSourceDataProvider.getBackfillStartWatermark(backfillSettings.backfillStartDate)
        endAt     <- dataProvider.currentWatermark
      yield (startFrom, endAt)
    }
    .flatMap { case (startWatermark, endWatermark) =>
      dataProvider.requestChanges(startWatermark, endWatermark)
    }
