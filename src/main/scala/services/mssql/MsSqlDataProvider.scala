package com.sneaksanddata.arcane.framework
package services.mssql

import models.app.PluginStreamContext
import models.schemas.DataRow
import models.settings.sink.SinkSettings
import models.settings.sources.SourceBufferingSettings
import models.settings.streaming.StreamModeSettings
import services.iceberg.base.SinkPropertyManager
import services.mssql.base.MsSqlReader
import services.mssql.versioning.MsSqlWatermark
import services.mssql.versioning.MsSqlWatermark.*
import services.streaming.base.DefaultSourceDataProvider
import services.streaming.throughput.base.ThroughputShaperBuilder

import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

import java.time.OffsetDateTime

/** A data provider that reads the changes from the Microsoft SQL Server.
  * @param reader
  *   The connection to the Microsoft SQL Server.
  */
class MsSqlDataProvider(
    reader: MsSqlReader,
    sinkPropertyManager: SinkPropertyManager,
    sinkSettings: SinkSettings,
    streamModeSettings: StreamModeSettings,
    throughputShaperBuilder: ThroughputShaperBuilder,
    sourceBufferingSettings: SourceBufferingSettings
) extends DefaultSourceDataProvider[MsSqlWatermark](
      sinkPropertyManager,
      sinkSettings,
      streamModeSettings,
      throughputShaperBuilder,
      sourceBufferingSettings
    ):

  def hasChanges(previousVersion: MsSqlWatermark): Task[Boolean] = reader.hasChanges(previousVersion)

  def getCurrentVersion(previousVersion: MsSqlWatermark): Task[MsSqlWatermark] =
    for
      // get current version from CHANGE_TRACKING_CURRENT_VERSION() and the commit time associated with it
      version <- reader
        .getVersion(QueryProvider.getCurrentVersionQuery)
        .flatMap {
          case Some(value) =>
            for commitTime <- reader.getVersionCommitTime(value)
            yield MsSqlWatermark.fromChangeTrackingVersion(value, commitTime)
          case None => ZIO.succeed(previousVersion)
        }
    yield version

  override protected def backfillStream(backfillStartDate: Option[OffsetDateTime]): ZStream[Any, Throwable, DataRow] =
    reader.backfill

  override protected def changeStream(previousVersion: MsSqlWatermark): ZStream[Any, Throwable, DataRow] =
    reader.getChanges(previousVersion)

  override protected def getBackfillStartWatermark(startTime: Option[OffsetDateTime]): MsSqlWatermark =
    MsSqlWatermark.epoch

/** The companion object for the MsSqlDataProvider class.
  */
object MsSqlDataProvider:

  /** The ZLayer that creates the MsSqlDataProvider.
    */
  val layer =
    ZLayer {
      for
        context         <- ZIO.service[PluginStreamContext]
        reader          <- ZIO.service[MsSqlReader]
        propertyManager <- ZIO.service[SinkPropertyManager]
        shaperBuilder   <- ZIO.service[ThroughputShaperBuilder]
      yield new MsSqlDataProvider(
        reader,
        propertyManager,
        context.sink,
        context.streamMode,
        shaperBuilder,
        context.source.buffering
      )
    }
