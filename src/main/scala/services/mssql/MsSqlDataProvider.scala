package com.sneaksanddata.arcane.framework
package services.mssql

import logging.ZIOLogAnnotations.zlog
import models.schemas.{DataRow, JsonWatermarkRow}
import models.settings.{BackfillSettings, SinkSettings, VersionedDataGraphBuilderSettings}
import services.iceberg.base.TablePropertyManager
import services.mssql.base.MsSqlReader
import services.mssql.versioning.MsSqlWatermark
import services.streaming.base.{BackfillDataProvider, VersionedDataProvider}

import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

import java.time.{Instant, OffsetDateTime, ZoneOffset}
import scala.util.Try

/** A data provider that reads the changes from the Microsoft SQL Server.
  * @param reader
  *   The connection to the Microsoft SQL Server.
  */
class MsSqlDataProvider(
    reader: MsSqlReader,
    propertyManager: TablePropertyManager,
    sinkSettings: SinkSettings,
    settings: VersionedDataGraphBuilderSettings,
    backfillSettings: BackfillSettings
) extends VersionedDataProvider[MsSqlWatermark, DataRow]
    with BackfillDataProvider[DataRow]:

  override def requestChanges(
      previousVersion: MsSqlWatermark,
      nextVersion: MsSqlWatermark
  ): ZStream[Any, Throwable, DataRow] =
    reader.getChanges(previousVersion).concat(ZStream.succeed(JsonWatermarkRow(nextVersion)))

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

  /** The first version of the data.
    */
  override def firstVersion: Task[MsSqlWatermark] =
    for
      watermarkString <- propertyManager.getProperty(sinkSettings.targetTableNameParts.Name, "comment")
      _ <- zlog("Current watermark value on %s is '%s'", sinkSettings.targetTableFullName, watermarkString)
      watermark <- ZIO
        .attempt(MsSqlWatermark.fromJson(watermarkString))
        .orDieWith(e =>
          new Throwable(
            s"Target contains invalid watermark: '$watermarkString'. Please run a backfill or update the watermark manually via COMMENT ON statement",
            e
          )
        )
    yield watermark

  /** Provides the backfill data.
    *
    * @return
    *   A task that represents the backfill data.
    */
  override def requestBackfill: ZStream[Any, Throwable, DataRow] = ZStream
    .fromZIO(getCurrentVersion(MsSqlWatermark.epoch))
    .flatMap(watermark => reader.backfill.concat(ZStream.succeed(JsonWatermarkRow(watermark))))

/** The companion object for the MsSqlDataProvider class.
  */
object MsSqlDataProvider:

  /** The ZLayer that creates the MsSqlDataProvider.
    */
  val layer =
    ZLayer {
      for
        reader            <- ZIO.service[MsSqlReader]
        versionedSettings <- ZIO.service[VersionedDataGraphBuilderSettings]
        propertyManager   <- ZIO.service[TablePropertyManager]
        sinkSettings      <- ZIO.service[SinkSettings]
        backfillSettings  <- ZIO.service[BackfillSettings]
      yield new MsSqlDataProvider(
        reader,
        propertyManager,
        sinkSettings,
        versionedSettings,
        backfillSettings
      )
    }
