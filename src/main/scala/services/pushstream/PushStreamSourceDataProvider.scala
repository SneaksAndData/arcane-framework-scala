package com.sneaksanddata.arcane.framework
package services.pushstream

import services.streaming.base.{DefaultSourceDataProvider, StructuredZStream}

import com.sneaksanddata.arcane.framework.models.schemas.{ArcaneSchema, DataRow}
import com.sneaksanddata.arcane.framework.models.settings.sink.SinkSettings
import com.sneaksanddata.arcane.framework.models.settings.sources.SourceBufferingSettings
import com.sneaksanddata.arcane.framework.services.iceberg.base.SinkPropertyManager
import com.sneaksanddata.arcane.framework.services.pushstream.versioning.PushStreamWatermark
import com.sneaksanddata.arcane.framework.services.streaming.throughput.base.ThroughputShaperBuilder
import zio.Task
import zio.stream.ZStream

class PushStreamSourceDataProvider(
    source: PushStreamingSource,
    sinkPropertyManager: SinkPropertyManager,
    sinkSettings: SinkSettings,
    throughputShaperBuilder: ThroughputShaperBuilder,
    sourceBufferingSettings: SourceBufferingSettings
) extends DefaultSourceDataProvider[PushStreamWatermark](
      sinkPropertyManager,
      sinkSettings,
      throughputShaperBuilder,
      sourceBufferingSettings
    ):
  override protected def changeStream(
      previousVersion: PushStreamWatermark
  ): ZStream[Any, Throwable, StructuredZStream] =
    source.getChanges(previousVersion)

  /** Checks whether the provided watermark from previous iteration has accrued any changes in [previousVersion ... now]
    * interval
    *
    * @param previousVersion
    *   Watermark from the previous change capture iteration
    * @return
    */
  override def hasChanges(previousVersion: PushStreamWatermark): Task[Boolean] = source.hasRows(previousVersion)

  /** Most recent version of a source dataset, compared. This should return previousVersion in case retrieval of a most
    * recent version failed.
    *
    * @param previousVersion
    *   Watermark from the previous change capture iteration
    * @return
    */
  override def getCurrentVersion(previousVersion: PushStreamWatermark): Task[PushStreamWatermark] = ???
