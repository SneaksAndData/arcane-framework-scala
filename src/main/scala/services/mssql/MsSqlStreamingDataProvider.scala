package com.sneaksanddata.arcane.framework
package services.mssql

import logging.ZIOLogAnnotations.zlog
import models.app.StreamContext
import models.schemas.{ArcaneType, DataCell, DataRow}
import models.settings.VersionedDataGraphBuilderSettings
import com.sneaksanddata.arcane.framework.services.mssql.base.MsSqlReader.{DataBatch, VersionedBatch}
import services.mssql.base.QueryResult
import services.streaming.base.StreamDataProvider

import zio.stream.ZStream
import zio.{ZIO, ZLayer}

import java.nio.ByteBuffer
import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}

/** Streaming data provider for Microsoft SQL Server. This provider relies on Change Tracking functionality of SQL
  * Server. For the provider to work correctly, source database must have Change Tracking enabled, and each streamed
  * source table must also have Change Tracking active. The provider relies on `sys.dm_tran_commit_table` to extract
  * EARLIEST version via `min(commit_ts)` available at commit_time > current watermark time. Given that multiple sources
  * indicate that `commit_ts` and CHANGE_TRACKING_CURRENT_VERSION() contain identical values:
  *
  * https://stackoverflow.com/questions/13821161/find-change-tracking-time-in-sql-server
  * https://www.brentozar.com/archive/2014/06/performance-tuning-sql-server-change-tracking/#:~:text=We've%20looked%20at%20which,MIN(commit_time)%20AS%20minimum_commit_time%2C
  * https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/change-tracking-sys-dm-tran-commit-table?view=sql-server-ver17
  * https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/change-tracking-current-version-transact-sql?view=sql-server-ver17
  *
  * the provider assumes that `commit_ts` can be used as watermark value interchangeable with the result from
  * CHANGE_TRACKING_CURRENT_VERSION(). This enables initial lookup on stream start from `sys.dm_tran_commit_table` that
  * continues into using CHANGE_TRACKING_CURRENT_VERSION() values that come from the stream output.
  */
class MsSqlStreamingDataProvider(
    dataProvider: MsSqlDataProvider,
    settings: VersionedDataGraphBuilderSettings,
    streamContext: StreamContext
) extends StreamDataProvider:

  type StreamElementType = DataRow

  /** @inheritdoc
    */
  override def stream: ZStream[Any, Throwable, DataRow] =
    val stream =
      if streamContext.IsBackfilling then dataProvider.requestBackfill
      else ZStream.unfoldZIO(None)(v => continueStream(v)).flatMap(readDataBatch)

    stream
      .map(_.handleSpecialTypes)

  extension (row: DataRow)
    private def handleSpecialTypes: DataRow =
      row.map {
        case DataCell(name, ArcaneType.TimestampType, value) if value != null =>
          DataCell(
            name,
            ArcaneType.TimestampType,
            LocalDateTime.ofInstant(value.asInstanceOf[Timestamp].toInstant, ZoneOffset.UTC)
          )

        case DataCell(name, ArcaneType.TimestampType, value) if value == null =>
          DataCell(name, ArcaneType.TimestampType, null)

        case DataCell(name, ArcaneType.ByteArrayType, value) if value != null =>
          DataCell(name, ArcaneType.ByteArrayType, ByteBuffer.wrap(value.asInstanceOf[Array[Byte]]))

        case DataCell(name, ArcaneType.ByteArrayType, value) if value == null =>
          DataCell(name, ArcaneType.ByteArrayType, null)

        case DataCell(name, ArcaneType.ShortType, value) =>
          DataCell(name, ArcaneType.IntType, value.asInstanceOf[Short].toInt)

        case DataCell(name, ArcaneType.DateType, value) =>
          DataCell(name, ArcaneType.DateType, value.asInstanceOf[java.sql.Date].toLocalDate)

        case other => other
      }

  private def continueStream(previousVersion: Option[Long]): ZIO[Any, Throwable, Some[(DataBatch, Option[Long])]] =
    for
      versionedBatch <- dataProvider.requestChanges(previousVersion, settings.lookBackInterval)
      latestVersion = versionedBatch.getLatestVersion
      _ <- zlog(s"Received versioned batch: $latestVersion")
      _ <- maybeSleep(versionedBatch)
      (queryResult, _) = versionedBatch
      _ <- zlog(s"Latest version: $latestVersion")
    yield Some(queryResult, latestVersion)

  private def maybeSleep(versionedBatch: VersionedBatch): ZIO[Any, Nothing, Unit] =
    versionedBatch match
      case (queryResult, _) =>
        val headOption = queryResult.read.nextOption()
        if headOption.isEmpty then
          zlog("No data in the batch, sleeping for the configured interval.") *> ZIO.sleep(
            settings.changeCaptureInterval
          )
        else zlog("Data found in the batch, continuing without sleep.") *> ZIO.unit

object MsSqlStreamingDataProvider:

  /** The environment for the MsSqlStreamingDataProvider.
    */
  type Environment = MsSqlDataProvider & VersionedDataGraphBuilderSettings & StreamContext

  /** Creates a new instance of the MsSqlStreamingDataProvider class.
    * @param dataProvider
    *   Underlying data provider.
    * @param settings
    *   The settings for the data graph builder.
    * @return
    *   A new instance of the MsSqlStreamingDataProvider class.
    */
  def apply(
      dataProvider: MsSqlDataProvider,
      settings: VersionedDataGraphBuilderSettings,
      streamContext: StreamContext
  ): MsSqlStreamingDataProvider =
    new MsSqlStreamingDataProvider(dataProvider, settings, streamContext)

  /** The ZLayer that creates the MsSqlStreamingDataProvider.
    */
  val layer: ZLayer[Environment, Nothing, StreamDataProvider] =
    ZLayer {
      for
        dataProvider  <- ZIO.service[MsSqlDataProvider]
        settings      <- ZIO.service[VersionedDataGraphBuilderSettings]
        streamContext <- ZIO.service[StreamContext]
      yield MsSqlStreamingDataProvider(dataProvider, settings, streamContext)
    }
