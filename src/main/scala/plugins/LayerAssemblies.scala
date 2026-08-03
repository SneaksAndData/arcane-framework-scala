package com.sneaksanddata.arcane.framework
package plugins

import models.app.PluginStreamContext
import services.app.base.{StreamLifetimeService, StreamRunnerService}
import services.app.{GenericStreamRunnerService, PosixStreamLifetimeService, StreamGraphResolver}
import services.backfill.DefaultBackfillStateManager
import services.backfill.base.{BackfillStreamDataProvider, ShardFactory, ShardedBackfillStreamDataProvider}
import services.backfill.processors.{BackfillCompletionProcessor, ShardStagingProcessor}
import services.base.{MergeServiceClient, StreamingSource}
import services.bootstrap.DefaultStreamBootstrapper
import services.completion.DefaultStreamFinalizer
import services.filters.FieldsFilteringService
import services.iceberg.base.{SinkEntityManager, SinkPropertyManager, StagingEntityManager, StagingPropertyManager}
import services.iceberg.{IcebergEntityManager, IcebergS3CatalogWriter, IcebergTablePropertyManager}
import services.merging.JdbcMergeServiceClient
import services.merging.cleanup.CatalogDisposeServiceClient
import services.metrics.{DataDog, DeclaredMetrics, GlobalMetricTagProvider}
import services.naming.{DefaultNameGenerator, NameGenerator}
import services.streaming.base.{StreamDataProvider, StreamingGraphBuilder}
import services.streaming.batching.StagedBatchFactory
import services.streaming.processors.batch_processors.maintenance.TargetMaintenanceProcessor
import services.streaming.processors.batch_processors.streaming.{
  DisposeBatchProcessor,
  MergeBatchProcessor,
  SchemaMigrationProcessor,
  WatermarkProcessor
}
import services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}

import com.sneaksanddata.arcane.framework.services.streaming.throughput.base.ThroughputShaperBuilder
import com.sneaksanddata.arcane.framework.services.synapse.backfill.{
  SynapseBackfillMergeStreamDataProvider,
  SynapseBackfillSourceDataProvider,
  SynapseShardFactory,
  SynapseShardedBackfillStreamDataProvider
}
import com.sneaksanddata.arcane.framework.services.synapse.base.{SynapseLinkDataProvider, SynapseLinkStreamingSource}
import com.sneaksanddata.arcane.framework.services.synapse.{SynapseBatchFactory, SynapseLinkStreamingDataProvider}
import zio.ZLayer
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig

object LayerAssemblies:
  type FrameworkServices = StreamRunnerService & StreamingGraphBuilder & DisposeBatchProcessor & MergeBatchProcessor &
    FieldFilteringTransformer & FieldsFilteringService & StreamLifetimeService & SinkPropertyManager &
    SinkEntityManager & StagingPropertyManager & StagingEntityManager & MergeServiceClient &
    DefaultBackfillStateManager & NameGenerator & DeclaredMetrics
  type PluginServices = PluginStreamContext & DatagramSocketConfig & MetricsConfig & DatadogPublisherConfig &
    StreamingSource & StagedBatchFactory & ShardFactory & ShardedBackfillStreamDataProvider & StreamDataProvider &
    BackfillStreamDataProvider

  type SynapseLinkServices = StreamingSource & StagedBatchFactory & ShardFactory & ShardedBackfillStreamDataProvider &
    StreamDataProvider & BackfillStreamDataProvider
  type FrameworkRequiredServices = SinkPropertyManager & DefaultBackfillStateManager & NameGenerator &
    PluginStreamContext & DeclaredMetrics

  lazy val synapseLinkSourceLayer
      : ZLayer[FrameworkRequiredServices & SynapseLinkStreamingSource, Throwable, SynapseLinkServices] =
    ZLayer.makeSome[FrameworkRequiredServices & SynapseLinkStreamingSource, SynapseLinkServices](
      // streaming
      SynapseLinkStreamingDataProvider.layer,
      SynapseBatchFactory.layer,
      SynapseShardFactory.layer,
      SynapseLinkDataProvider.layer,

      // backfill
      SynapseBackfillSourceDataProvider.layer,
      SynapseShardedBackfillStreamDataProvider.layer,
      SynapseBackfillMergeStreamDataProvider.layer,
      ThroughputShaperBuilder.layer
    )

  lazy val frameworkServicesLayer: ZLayer[PluginServices, Throwable, FrameworkServices] =
    ZLayer.makeSome[PluginServices, FrameworkServices](
      GenericStreamRunnerService.layer,
      StreamGraphResolver.composedLayer,
      DisposeBatchProcessor.layer,
      FieldFilteringTransformer.layer,
      MergeBatchProcessor.layer,
      StagingProcessor.layer,
      FieldsFilteringService.layer,
      PosixStreamLifetimeService.layer,
      IcebergS3CatalogWriter.layer,
      IcebergEntityManager.sinkLayer,
      IcebergEntityManager.stagingLayer,
      IcebergTablePropertyManager.stagingLayer,
      IcebergTablePropertyManager.sinkLayer,
      JdbcMergeServiceClient.layer,

      // schema
      SchemaMigrationProcessor.layer,

      // maintenance and cleanup
      TargetMaintenanceProcessor.layer,
      CatalogDisposeServiceClient.layer,
      DefaultNameGenerator.layer,
      DeclaredMetrics.layer,
      WatermarkProcessor.layer,
      DefaultStreamBootstrapper.layer,
      GlobalMetricTagProvider.layer,
      DataDog.UdsPublisher.layer,
      DefaultStreamFinalizer.layer,

      // backfill
      DefaultBackfillStateManager.layer,
      ShardStagingProcessor.layer,
      BackfillCompletionProcessor.layer
    )
