package com.sneaksanddata.arcane.framework
package plugins

import services.app.base.{StreamLifetimeService, StreamRunnerService}
import services.backfill.DefaultBackfillStateManager
import services.backfill.base.{BackfillStreamDataProvider, ShardFactory, ShardedBackfillStreamDataProvider}
import services.base.MergeServiceClient
import services.filters.FieldsFilteringService
import services.iceberg.base.{SinkEntityManager, SinkPropertyManager, StagingEntityManager, StagingPropertyManager}
import services.metrics.DeclaredMetrics
import services.naming.NameGenerator
import services.streaming.base.{StreamDataProvider, StreamingGraphBuilder}
import services.streaming.batching.StagedBatchFactory
import services.streaming.processors.batch_processors.streaming.{DisposeBatchProcessor, MergeBatchProcessor}
import services.streaming.processors.transformers.FieldFilteringTransformer

import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig

type FrameworkServices = StreamRunnerService & StreamingGraphBuilder & DisposeBatchProcessor & MergeBatchProcessor &
  FieldFilteringTransformer & FieldsFilteringService & StreamLifetimeService & SinkPropertyManager & SinkEntityManager &
  StagingPropertyManager & StagingEntityManager & MergeServiceClient & DefaultBackfillStateManager & NameGenerator &
  DeclaredMetrics
type PluginServices = DatagramSocketConfig & MetricsConfig & DatadogPublisherConfig & StagedBatchFactory &
  ShardFactory & ShardedBackfillStreamDataProvider & StreamDataProvider & BackfillStreamDataProvider

type SynapseLinkServices = StagedBatchFactory & ShardFactory & ShardedBackfillStreamDataProvider & StreamDataProvider &
  BackfillStreamDataProvider

type FrameworkRequiredServices = SinkPropertyManager & DefaultBackfillStateManager & NameGenerator & DeclaredMetrics
