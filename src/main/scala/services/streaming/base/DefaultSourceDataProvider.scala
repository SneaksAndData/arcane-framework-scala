package com.sneaksanddata.arcane.framework
package services.streaming.base

import logging.ZIOLogAnnotations.{zlog, zlogStream}
import models.schemas.{ArcaneSchema, DataRow, JsonWatermarkRow}
import models.settings.TableNaming.*
import models.settings.sink.SinkSettings
import models.settings.sources.SourceBufferingSettings
import models.settings.streaming.StreamModeSettings
import models.settings.sources.{BufferingImpl, UnboundedImpl}
import services.iceberg.base.SinkPropertyManager
import services.streaming.throughput.base.ThroughputShaperBuilder

import com.sneaksanddata.arcane.framework.services.backfill.BackfillSourceDataProvider
import upickle.ReadWriter
import zio.stream.ZStream
import zio.{Task, ZIO}

import java.time.OffsetDateTime

/** Default implementations for source data emitter used by StreamDataProvider
  * @tparam WatermarkType
  *   Watermark implementation for the source
  */
abstract class DefaultSourceDataProvider[WatermarkType <: SourceWatermark[String] & JsonWatermark](
    sinkPropertyManager: SinkPropertyManager,
    sinkSettings: SinkSettings,
    streamMode: StreamModeSettings,
    throughputShaperBuilder: ThroughputShaperBuilder,
    sourceBufferingSettings: SourceBufferingSettings
)(implicit rw: ReadWriter[WatermarkType])
    extends ChangeCaptureDataProvider[WatermarkType]:

  private val throughputShaper = throughputShaperBuilder.build

  /** Implements data streaming logic for public `requestChanges`
    *
    * @param previousVersion
    *   Previous watermark
    * @return
    */
  protected def changeStream(
      previousVersion: WatermarkType
  ): ZStream[Any, Throwable, StructuredZStream]
  
  final override def requestChanges(
      previousVersion: WatermarkType,
      nextVersion: WatermarkType
  ): ZStream[Any, Throwable, StructuredZStream] = changeStream(previousVersion).map(changeSet =>
    (
      throughputShaper
        .shapeStream(trySetBuffering(changeSet._1))
        .concat(ZStream.succeed(JsonWatermarkRow(nextVersion))),
      changeSet._2
    )
  )

  override def firstVersion: Task[WatermarkType] = for
    watermarkString <- sinkPropertyManager.getProperty(sinkSettings.targetTableFullName.parts.name, "comment")
    _               <- zlog("Current watermark value on %s is '%s'", sinkSettings.targetTableFullName, watermarkString)
    watermark <- ZIO
      .attempt(upickle.read(watermarkString))
      .orDieWith(e =>
        new Throwable(
          s"Target contains invalid watermark: '$watermarkString'. Please run a backfill or update the watermark manually via COMMENT ON statement",
          e
        )
      )
  yield watermark

  private def trySetBuffering(stream: ZStream[Any, Throwable, DataRow]): ZStream[Any, Throwable, DataRow] =
    (sourceBufferingSettings.bufferingEnabled, sourceBufferingSettings.bufferingStrategy) match
      case (true, UnboundedImpl(_)) =>
        zlogStream("Running stream with unbound source buffer") *> stream.bufferUnbounded

      case (true, BufferingImpl(buffering)) =>
        zlogStream("Running stream with bound source buffer size %s", buffering.maxBufferSize.toString) *> stream
          .buffer(buffering.maxBufferSize)

      case (false, _) => zlogStream("Running stream with disabled source buffering") *> stream
