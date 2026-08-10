package com.sneaksanddata.arcane.framework
package plugins

import services.app.base.StreamLifetimeService
import services.backfill.base.{
  BackfillStateManager,
  BackfillStreamDataProvider,
  ShardFactory,
  ShardedBackfillStreamDataProvider
}
import services.backfill.processors.{BackfillCompletionProcessor, ShardStagingProcessor}
import services.base.{MergeServiceClient, StreamingSource}
import services.bootstrap.base.StreamBootstrapper
import services.completion.base.StreamFinalizer
import services.filters.FieldsFilteringService
import services.iceberg.IcebergS3CatalogWriter
import services.iceberg.base.{SinkEntityManager, SinkPropertyManager, StagingEntityManager, StagingPropertyManager}
import services.metrics.DeclaredMetrics
import services.metrics.base.MetricTagProvider
import services.naming.NameGenerator
import services.streaming.base.StreamDataProvider
import services.streaming.batching.StagedBatchFactory
import services.streaming.processors.batch_processors.maintenance.TargetMaintenanceProcessor
import services.streaming.processors.batch_processors.streaming.{
  DisposeBatchProcessor,
  MergeBatchProcessor,
  SchemaMigrationProcessor,
  WatermarkProcessor
}
import services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}

type FrameworkProvidedPipelineServices = DisposeBatchProcessor & MergeBatchProcessor & FieldFilteringTransformer &
  FieldsFilteringService & StreamLifetimeService & SinkPropertyManager & SinkEntityManager & StagingPropertyManager &
  StagingEntityManager & MergeServiceClient & NameGenerator & DeclaredMetrics & StreamBootstrapper & StreamFinalizer &
  MetricTagProvider & IcebergS3CatalogWriter & WatermarkProcessor & TargetMaintenanceProcessor &
  SchemaMigrationProcessor

type FrameworkRequiredStagingServices = StagedBatchFactory & IcebergS3CatalogWriter & ShardFactory & DeclaredMetrics &
  NameGenerator & SinkPropertyManager & MergeServiceClient
type FrameworkProvidedStagingServices = ShardStagingProcessor & BackfillCompletionProcessor & StagingProcessor

type BlobListRequiredServices = SinkPropertyManager & StagingEntityManager & StagingPropertyManager & NameGenerator &
  DeclaredMetrics
type BlobListProvidedServices = StagedBatchFactory & ShardFactory & ShardedBackfillStreamDataProvider &
  StreamDataProvider & BackfillStreamDataProvider & BackfillStateManager
