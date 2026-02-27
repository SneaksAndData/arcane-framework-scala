package com.sneaksanddata.arcane.framework
package services.mssql

import models.app.StreamContext
import models.settings.VersionedDataGraphBuilderSettings
import models.settings.backfill.BackfillSettings
import services.metrics.DeclaredMetrics
import services.mssql.versioning.MsSqlWatermark
import services.streaming.base.{DefaultStreamDataProvider, StreamDataProvider}

import zio.{ZIO, ZLayer}

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
  * CHANGE_TRACKING_CURRENT_VERSION(). This enables commit time lookups from `sys.dm_tran_commit_table` that continues
  * into using CHANGE_TRACKING_CURRENT_VERSION() values that come from the stream output.
  */
class MsSqlStreamingDataProvider(
    dataProvider: MsSqlDataProvider,
    settings: VersionedDataGraphBuilderSettings,
    backfillSettings: BackfillSettings,
    streamContext: StreamContext,
    declaredMetrics: DeclaredMetrics
) extends DefaultStreamDataProvider[MsSqlWatermark, MsSqlBatch](
      dataProvider,
      settings,
      backfillSettings,
      streamContext,
      declaredMetrics
    )

object MsSqlStreamingDataProvider:

  /** The environment for the MsSqlStreamingDataProvider.
    */
  type Environment = MsSqlDataProvider & VersionedDataGraphBuilderSettings & BackfillSettings & StreamContext &
    DeclaredMetrics

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
      backfillSettings: BackfillSettings,
      streamContext: StreamContext,
      declaredMetrics: DeclaredMetrics
  ): MsSqlStreamingDataProvider =
    new MsSqlStreamingDataProvider(dataProvider, settings, backfillSettings, streamContext, declaredMetrics)

  /** The ZLayer that creates the MsSqlStreamingDataProvider.
    */
  val layer: ZLayer[Environment, Nothing, StreamDataProvider] =
    ZLayer {
      for
        dataProvider     <- ZIO.service[MsSqlDataProvider]
        settings         <- ZIO.service[VersionedDataGraphBuilderSettings]
        backfillSettings <- ZIO.service[BackfillSettings]
        streamContext    <- ZIO.service[StreamContext]
        declaredMetrics  <- ZIO.service[DeclaredMetrics]
      yield MsSqlStreamingDataProvider(dataProvider, settings, backfillSettings, streamContext, declaredMetrics)
    }
