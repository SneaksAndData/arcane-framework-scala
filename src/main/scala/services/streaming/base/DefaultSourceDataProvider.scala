package com.sneaksanddata.arcane.framework
package services.streaming.base

import extensions.ZExtensions.trySetBuffering
import logging.ZIOLogAnnotations.zlog
import models.MetadataKeys
import models.schemas.{ArcaneSchema, JsonWatermarkRow}
import models.settings.TableNaming.*
import models.settings.sink.SinkSettings
import models.settings.sources.SourceBufferingSettings
import services.base.StreamingSource
import services.iceberg.base.SinkPropertyManager
import services.metrics.DeclaredMetrics
import services.streaming.throughput.base.ThroughputShaperBuilder

import upickle.ReadWriter
import zio.stream.ZStream
import zio.{Task, ZIO}

/** Default implementations for source data emitter used by StreamDataProvider
  * @tparam WatermarkType
  *   Watermark implementation for the source
  */
abstract class DefaultSourceDataProvider[WatermarkType <: SourceWatermark[String] & JsonWatermark](
    streamingSource: StreamingSource,
    sinkPropertyManager: SinkPropertyManager,
    sinkSettings: SinkSettings,
    throughputShaperBuilder: ThroughputShaperBuilder,
    sourceBufferingSettings: SourceBufferingSettings,
    declaredMetrics: DeclaredMetrics
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
          .shapeStream(
            changeSet._1
              .trySetBuffering(sourceBufferingSettings)
              .tap(_ => ZIO.succeed(1L) @@ declaredMetrics.rowsIncoming)
          ),
        changeSet._2
      )
    )
    .concat(ZStream.succeed((ZStream.succeed(JsonWatermarkRow(nextVersion)), ArcaneSchema.empty())))

  final override def currentWatermark: Task[WatermarkType] = for
    watermarkString <-
      for
        watermarkExpectedString <- sinkPropertyManager.getProperty(
          sinkSettings.targetTableFullName.parts.name,
          MetadataKeys.watermarkKey
        )
        watermarkResolvedString <- ZIO.ifZIO(ZIO.succeed(watermarkExpectedString.isEmpty))(
          onTrue = for
            _ <- zlog(
              s"Reading watermark using legacy key (${MetadataKeys.legacyWatermarkKey}) - new values will be saved under a new (${MetadataKeys.watermarkKey})"
            )
            legacyValue <- sinkPropertyManager.getRequiredProperty(
              sinkSettings.targetTableFullName.parts.name,
              MetadataKeys.legacyWatermarkKey
            )
            _ <- sinkPropertyManager.setProperty(
              sinkSettings.targetTableFullName.parts.name,
              MetadataKeys.watermarkKey,
              legacyValue
            )
          yield legacyValue,
          onFalse = ZIO.attempt(watermarkExpectedString.get)
        )
      yield watermarkResolvedString
    _ <- zlog("Current watermark value on %s is '%s'", sinkSettings.targetTableFullName, watermarkString)
    watermark <- ZIO
      .attempt(upickle.read(watermarkString))
      .orDieWith(e =>
        new Throwable(
          s"Invalid watermark value: '$watermarkString'. Please run a backfill or update the watermark manually via COMMENT ON statement",
          e
        )
      )
  yield watermark
