package com.sneaksanddata.arcane.framework
package services.streaming.base

import logging.ZIOLogAnnotations.zlog
import models.schemas.{DataRow, JsonWatermarkRow}
import models.settings.backfill.BackfillSettings
import models.settings.sink.SinkSettings
import models.settings.streaming.StreamModeSettings
import services.iceberg.base.SinkPropertyManager
import services.streaming.throughput.base.ThroughputShaperBuilder

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
    throughputShaperBuilder: ThroughputShaperBuilder
)(implicit rw: ReadWriter[WatermarkType])
    extends VersionedDataProvider[WatermarkType, DataRow]
    with BackfillDataProvider[DataRow]:

  private val throughputShaper = throughputShaperBuilder.build

  /** Implements data streaming logic for public `requestChanges`
    *
    * @param previousVersion
    *   Previous watermark
    * @return
    */
  protected def changeStream(
      previousVersion: WatermarkType
  ): ZStream[Any, Throwable, DataRow]

  /** Evaluates watermark to be used when evaluating current snapshot version at the start of a backfill process
    *
    * @param startTime
    * @return
    */
  protected def getBackfillStartWatermark(startTime: Option[OffsetDateTime]): WatermarkType

  /** Implements data streaming logic for public `requestBackfill`
    *
    * @param backfillStartDate
    * @return
    */
  protected def backfillStream(backfillStartDate: Option[OffsetDateTime]): ZStream[Any, Throwable, DataRow]

  final override def requestChanges(
      previousVersion: WatermarkType,
      nextVersion: WatermarkType
  ): ZStream[Any, Throwable, DataRow] = throughputShaper
    .shapeStream(changeStream(previousVersion))
    .concat(ZStream.succeed(JsonWatermarkRow(nextVersion)))

  final override def requestBackfill: ZStream[Any, Throwable, DataRow] = ZStream
    .fromZIO(getCurrentVersion(getBackfillStartWatermark(streamMode.backfill.backfillStartDate)))
    .flatMap(version =>
      throughputShaper
        .shapeStream(backfillStream(streamMode.backfill.backfillStartDate))
        .concat(ZStream.succeed(JsonWatermarkRow(version)))
    )

  override def firstVersion: Task[WatermarkType] = for
    watermarkString <- sinkPropertyManager.getProperty(sinkSettings.targetTableNameParts.Name, "comment")
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
