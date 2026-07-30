package com.sneaksanddata.arcane.framework
package services.streaming.base

import extensions.ZExtensions.trySetBuffering
import logging.ZIOLogAnnotations.zlog
import models.schemas.{ArcaneSchema, JsonWatermarkRow}
import models.settings.TableNaming.*
import models.settings.sink.SinkSettings
import models.settings.sources.SourceBufferingSettings
import services.iceberg.base.SinkPropertyManager
import services.streaming.throughput.base.ThroughputShaperBuilder

import upickle.ReadWriter
import zio.stream.ZStream
import zio.{Task, ZIO}

/** Default implementations for source data emitter used by StreamDataProvider
  * @tparam WatermarkType
  *   Watermark implementation for the source
  */
abstract class DefaultSourceDataProvider[WatermarkType <: SourceWatermark[String] & JsonWatermark](
    sinkPropertyManager: SinkPropertyManager,
    sinkSettings: SinkSettings,
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
  ): ZStream[Any, Throwable, StructuredZStream] = changeStream(previousVersion)
    .map(changeSet =>
      (
        throughputShaper
          .shapeStream(changeSet._1.trySetBuffering(sourceBufferingSettings)),
        changeSet._2
      )
    )
    .concat(ZStream.succeed((ZStream.succeed(JsonWatermarkRow(nextVersion)), ArcaneSchema.empty())))

  final override def currentWatermark: Task[WatermarkType] = for
    watermarkString <- sinkPropertyManager.getRequiredProperty(sinkSettings.targetTableFullName.parts.name, "comment")
    _               <- zlog("Current watermark value on %s is '%s'", sinkSettings.targetTableFullName, watermarkString)
    watermark <- ZIO
      .attempt(upickle.read(watermarkString))
      .orDieWith(e =>
        new Throwable(
          s"Invalid watermark value: '$watermarkString'. Please run a backfill or update the watermark manually via COMMENT ON statement",
          e
        )
      )
  yield watermark
