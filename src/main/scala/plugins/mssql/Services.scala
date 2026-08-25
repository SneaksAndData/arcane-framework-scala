package com.sneaksanddata.arcane.framework
package plugins.mssql

import services.backfill.base.{
  BackfillStateManager,
  BackfillStreamDataProvider,
  ShardFactory,
  ShardedBackfillStreamDataProvider
}
import services.iceberg.base.{SinkPropertyManager, StagingEntityManager, StagingPropertyManager}
import services.metrics.DeclaredMetrics
import services.naming.NameGenerator
import services.streaming.base.StreamDataProvider
import services.streaming.batching.StagedBatchFactory

import models.app.PluginStreamContext
import services.backfill.DefaultBackfillStateManager
import services.mssql.{MsSqlDataProvider, MsSqlStagedBatchFactory, MsSqlStreamingDataProvider}
import services.mssql.backfill.{
  MsSqlBackfillMergeStreamDataProvider,
  MsSqlBackfillSourceDataProvider,
  MsSqlShardFactory,
  MsSqlShardedBackfillStreamDataProvider
}
import services.mssql.base.MsSqlStreamingSource
import services.streaming.throughput.base.ThroughputShaperBuilder

import zio.ZLayer

type MsSqlRequiredServices = SinkPropertyManager & StagingEntityManager & StagingPropertyManager & NameGenerator &
  DeclaredMetrics
type MsSqlProvidedServices = StagedBatchFactory & ShardFactory & ShardedBackfillStreamDataProvider &
  StreamDataProvider & BackfillStreamDataProvider & BackfillStateManager

object Services:
  lazy val mssqlSourceLayer: ZLayer[
    MsSqlRequiredServices & MsSqlStreamingSource & PluginStreamContext,
    Throwable,
    MsSqlProvidedServices
  ] = ZLayer.makeSome[MsSqlRequiredServices & MsSqlStreamingSource & PluginStreamContext, MsSqlProvidedServices](
    MsSqlDataProvider.layer,
    MsSqlStreamingDataProvider.layer,
    MsSqlStagedBatchFactory.layer,
    MsSqlBackfillSourceDataProvider.layer,
    MsSqlBackfillMergeStreamDataProvider.layer,
    MsSqlShardedBackfillStreamDataProvider.layer,
    MsSqlShardFactory.layer,
    ThroughputShaperBuilder.layer,
    DefaultBackfillStateManager.layer
  )
