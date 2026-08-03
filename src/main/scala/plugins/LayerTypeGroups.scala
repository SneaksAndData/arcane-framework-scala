package com.sneaksanddata.arcane.framework
package plugins

import models.app.PluginStreamContext
import services.app.base.{StreamLifetimeService, StreamRunnerService}
import services.backfill.DefaultBackfillStateManager
import services.backfill.base.{
  BackfillStateManager,
  BackfillStreamDataProvider,
  ShardFactory,
  ShardedBackfillStreamDataProvider
}
import services.base.{MergeServiceClient, StreamingSource}
import services.filters.FieldsFilteringService
import services.iceberg.base.{SinkEntityManager, SinkPropertyManager, StagingEntityManager, StagingPropertyManager}
import services.metrics.DeclaredMetrics
import services.naming.NameGenerator
import services.streaming.base.{StreamDataProvider, StreamingGraphBuilder}
import services.streaming.batching.StagedBatchFactory
import services.streaming.processors.batch_processors.streaming.{DisposeBatchProcessor, MergeBatchProcessor}
import services.streaming.processors.transformers.FieldFilteringTransformer

type FrameworkProvidedServices = StreamRunnerService & StreamingGraphBuilder & DisposeBatchProcessor &
  MergeBatchProcessor & FieldFilteringTransformer & FieldsFilteringService & StreamLifetimeService &
  SinkPropertyManager & SinkEntityManager & StagingPropertyManager & StagingEntityManager & MergeServiceClient &
  NameGenerator & DeclaredMetrics
type PluginRequiredServices = StagedBatchFactory & ShardFactory & ShardedBackfillStreamDataProvider &
  StreamDataProvider & BackfillStreamDataProvider & BackfillStateManager & StreamingSource

type SynapseLinkProvidedServices = StagedBatchFactory & ShardFactory & ShardedBackfillStreamDataProvider &
  StreamDataProvider & BackfillStreamDataProvider & BackfillStateManager & StreamingSource

type SynapseLinkSourceRequiredServices = SinkPropertyManager & DefaultBackfillStateManager & NameGenerator &
  DeclaredMetrics
