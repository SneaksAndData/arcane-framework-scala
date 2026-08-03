package com.sneaksanddata.arcane.framework
package plugins

import models.app.PluginStreamContext
import services.app.base.StreamRunnerService
import services.app.{GenericStreamRunnerService, PosixStreamLifetimeService, StreamGraphResolver}
import services.backfill.DefaultBackfillStateManager
import services.backfill.base.{BackfillStreamDataProvider, ShardFactory, ShardedBackfillStreamDataProvider}
import services.backfill.processors.{BackfillCompletionProcessor, ShardStagingProcessor}
import services.base.StreamingSource
import services.bootstrap.DefaultStreamBootstrapper
import services.completion.DefaultStreamFinalizer
import services.filters.FieldsFilteringService
import services.iceberg.base.SinkPropertyManager
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
import services.streaming.throughput.base.ThroughputShaperBuilder
import services.synapse.backfill.{
  SynapseBackfillMergeStreamDataProvider,
  SynapseBackfillSourceDataProvider,
  SynapseShardFactory,
  SynapseShardedBackfillStreamDataProvider
}
import services.synapse.base.{SynapseLinkDataProvider, SynapseLinkStreamingSource}
import services.synapse.{SynapseBatchFactory, SynapseLinkStreamingDataProvider}

import zio.ZLayer
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig

object LayerAssemblies:

  lazy val synapseLinkSourceLayer: ZLayer[
    SynapseLinkSourceRequiredServices & SynapseLinkStreamingSource & PluginStreamContext,
    Throwable,
    SynapseLinkProvidedServices
  ] =
    ZLayer.makeSome[
      SynapseLinkSourceRequiredServices & SynapseLinkStreamingSource & PluginStreamContext,
      SynapseLinkProvidedServices
    ](
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

  lazy val frameworkServicesLayer: ZLayer[PluginRequiredServices, Throwable, FrameworkProvidedServices] =
    ZLayer.makeSome[PluginRequiredServices, FrameworkProvidedServices](
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
      ShardStagingProcessor.layer,
      BackfillCompletionProcessor.layer
    )
