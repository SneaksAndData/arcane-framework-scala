package com.sneaksanddata.arcane.framework
package services.mssql

import logging.ZIOLogAnnotations.zlog
import models.app.StreamContext
import models.schemas.DataRow
import models.settings.VersionedDataGraphBuilderSettings
import services.streaming.base.StreamDataProvider

import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

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
    val stream = {
      // simply launch backfill if stream context specifies so
      if streamContext.IsBackfilling then dataProvider.requestBackfill
      // in streaming mode, provide a continuous stream of (current, previous) versions
      else
        ZStream
          .unfoldZIO(dataProvider.firstVersion) { version =>
            for
              previousVersion   <- version
              currentVersion    <- dataProvider.getCurrentVersion(previousVersion)
              hasVersionUpdated <- ZIO.succeed(previousVersion.versionNumber == currentVersion.versionNumber)
              _ <- ZIO.when(hasVersionUpdated) {
                for
                  _ <- zlog(
                    s"Database change tracking version updated from $previousVersion to $currentVersion, checking if source has changes"
                  )
                  _ <- checkEmpty(previousVersion)
                yield ()
              }
              _ <- ZIO.unless(hasVersionUpdated) {
                for _ <- zlog(
                    s"Database change tracking version $previousVersion unchanged, no new data is available for streaming. Next check in ${settings.changeCaptureInterval.toSeconds} seconds"
                  ) *> ZIO.sleep(
                    settings.changeCaptureInterval
                  )
                yield ()
              }
            yield Some((currentVersion, previousVersion) -> ZIO.succeed(currentVersion))
          }
          .flatMap {
            // always fetch from previousVersion to ensure data changes that happened during the last iteration get captured
            case (currentVersion, previousVersion) if currentVersion.versionNumber > previousVersion.versionNumber =>
              dataProvider.requestChanges(previousVersion)
            // skip emit if the version hasn't changed
            case _ => ZStream.empty
          }
    }

    stream
      .map(_.handleSpecialTypes)

  private def checkEmpty(previousVersion: MsSqlChangeVersion): Task[Unit] =
    for
      _         <- zlog(s"Received versioned batch: ${previousVersion.versionNumber}")
      isChanged <- dataProvider.hasChanges(previousVersion)
      _ <- ZIO.unless(isChanged) {
        zlog(
          s"No data in the batch, next check in ${settings.changeCaptureInterval.toSeconds} seconds"
        ) *> ZIO.sleep(
          settings.changeCaptureInterval
        )
      }
      _ <- ZIO.when(isChanged) {
        zlog(s"Data found in the batch: ${previousVersion.versionNumber}, continuing") *> ZIO.unit
      }
    yield ()

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
