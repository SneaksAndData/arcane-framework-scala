package com.sneaksanddata.arcane.framework
package plugins

import models.app.PluginStreamContext
import services.app.PosixStreamLifetimeService
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
import services.streaming.processors.batch_processors.maintenance.TargetMaintenanceProcessor
import services.streaming.processors.batch_processors.streaming.{
  DisposeBatchProcessor,
  MergeBatchProcessor,
  SchemaMigrationProcessor,
  WatermarkProcessor
}
import services.streaming.processors.transformers.{FieldFilteringTransformer, StagingProcessor}

import zio.ZLayer
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig

object LayerAssemblies:

  lazy val frameworkPipelineServicesLayer: ZLayer[
    PluginStreamContext.PluginConfiguration & StreamingSource,
    Throwable,
    FrameworkProvidedPipelineServices
  ] =
    ZLayer.makeSome[PluginStreamContext.PluginConfiguration & StreamingSource, FrameworkProvidedPipelineServices](
      DisposeBatchProcessor.layer,
      FieldFilteringTransformer.layer,
      MergeBatchProcessor.layer,
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
      DefaultStreamFinalizer.layer
    )

  lazy val frameworkStagingServicesLayer
      : ZLayer[FrameworkRequiredStagingServices & PluginStreamContext, Nothing, FrameworkProvidedStagingServices] =
    ZLayer.makeSome[FrameworkRequiredStagingServices & PluginStreamContext, FrameworkProvidedStagingServices](
      ShardStagingProcessor.layer,
      BackfillCompletionProcessor.layer,
      StagingProcessor.layer
    )
