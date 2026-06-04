package com.sneaksanddata.arcane.framework
package services.backfill

import models.settings.backfill.BackfillSettings
import services.backfill.base.BackfillStreamDataProvider
import services.streaming.base.{ChangeCaptureDataProvider, JsonWatermark, SourceWatermark, StructuredZStream}

import zio.stream.ZStream

class DefaultBackfillMergeStreamDataProvider[WatermarkType <: SourceWatermark[String] & JsonWatermark](
                                                                                                        dataProvider: ChangeCaptureDataProvider[WatermarkType],
                                                                                                        backfillSettings: BackfillSettings
                                                                                                      ) extends BackfillStreamDataProvider:
  /** Returns the stream of elements.
   */
  override def stream: ZStream[Any, Throwable, StructuredZStream] = ???

