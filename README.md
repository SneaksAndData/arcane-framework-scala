# Arcane Framework (Scala - ZIO)
Arcane — A simple ZIO-based data streaming framework that seamlessly runs locally and on Kubernetes with the support of [Arcane Operator](https://github.com/sneaksanddata/arcane-operator)

## Creating plugins
Plugins utilize framework to create specific source-to-Iceberg streaming applications.
Developers should create a Scala3 project for each plugin, containing stream context definition, source settings definition and `main.scala`.

### Stream source definition
First, you should create a serializable proxy for plugin source configuration:
```scala 3
case class MySourceSettings(
    override val buffering: DefaultSourceBufferingSettings,
    override val fieldSelectionRule: DefaultFieldSelectionRuleSettings,
    override val configuration: DefaultMySourceSettings
) extends StreamSourceSettings derives ReadWriter:
  override type SourceSettingsType = DefaultMySourceSettings
```

where `DefaultMySourceSettings` should be defined in the framework, but you can also create one in the plugin repository.

### Stream context definition
Most plugins can rely on framework defaults and the stream source created on the previous step:
```scala 3
import com.sneaksanddata.arcane.framework.models.app.{DefaultPluginStreamContext, PluginStreamContext}
import com.sneaksanddata.arcane.framework.models.settings.observability.DefaultObservabilitySettings
import com.sneaksanddata.arcane.framework.models.settings.sink.DefaultSinkSettings
import com.sneaksanddata.arcane.framework.models.settings.staging.DefaultStagingSettings
import com.sneaksanddata.arcane.framework.models.settings.streaming.{
  DefaultStreamModeSettings,
  DefaultThroughputSettings
}

case class MyPluginStreamContext(
    @key("observability") private val observabilityIn: DefaultObservabilitySettings,
    @key("staging") private val stagingIn: DefaultStagingSettings,
    @key("streamMode") private val streamModeIn: DefaultStreamModeSettings,
    @key("sink") private val sinkIn: DefaultSinkSettings,
    @key("throughput") private val throughputIn: DefaultThroughputSettings,
    override val source: MyPluginSourceSettings
) extends DefaultPluginStreamContext(observabilityIn, stagingIn, streamModeIn, sinkIn, throughputIn) derives ReadWriter:
  // TODO: should be implemented when Operator supports overrides
  override def merge(other: Option[PluginStreamContext]): PluginStreamContext = this

object MyPluginStreamContext:
  def apply(value: String): MyPluginStreamContext =
    PluginStreamContext[MyPluginStreamContext](value)

  // ZLayer for injecting the stream context singleton
  lazy val layer
      : ZLayer[Any, Throwable, PluginStreamContext & DatagramSocketConfig & MetricsConfig & DatadogPublisherConfig] =
    PluginStreamContext.getLayer[MyPluginStreamContext]
```

### Entrypoint

Now you can add `main.scala` and work is done:
```scala 3
package com.sneaksanddata.arcane.sql_server_change_tracking

import models.app.MicrosoftSqlServerPluginStreamContext

import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import com.sneaksanddata.arcane.framework.models.schemas.ArcaneSchema
import com.sneaksanddata.arcane.framework.services.app.base.{StreamLifetimeService, StreamRunnerService}
import com.sneaksanddata.arcane.framework.services.app.{GenericStreamRunnerService, PosixStreamLifetimeService}
import com.sneaksanddata.arcane.framework.services.base.SchemaProvider
import com.sneaksanddata.arcane.framework.services.bootstrap.DefaultStreamBootstrapper
import com.sneaksanddata.arcane.framework.services.filters.{ColumnSummaryFieldsFilteringService, FieldsFilteringService}
import com.sneaksanddata.arcane.framework.services.iceberg.{
  IcebergEntityManager,
  IcebergS3CatalogWriter,
  IcebergTablePropertyManager
}
import com.sneaksanddata.arcane.framework.services.merging.JdbcMergeServiceClient
import com.sneaksanddata.arcane.framework.services.metrics.{ArcaneDimensionsProvider, DeclaredMetrics}
import com.sneaksanddata.arcane.framework.services.streaming.data_providers.backfill.{
  GenericBackfillStreamingMergeDataProvider,
  GenericBackfillStreamingOverwriteDataProvider
}
import com.sneaksanddata.arcane.framework.services.streaming.graph_builders.{
  GenericGraphBuilderFactory,
  GenericStreamingGraphBuilder
}
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.backfill.{
  BackfillApplyBatchProcessor,
  BackfillOverwriteWatermarkProcessor
}
import com.sneaksanddata.arcane.framework.services.streaming.processors.batch_processors.streaming.{
  DisposeBatchProcessor,
  MergeBatchProcessor,
  WatermarkProcessor
}
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.{
  FieldFilteringTransformer,
  StagingProcessor
}
import com.sneaksanddata.arcane.framework.services.streaming.throughput.base.ThroughputShaperBuilder
import zio.logging.backend.SLF4J
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.datadog.DatadogPublisherConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig
import zio.metrics.jvm.DefaultJvmMetrics
import zio.{Runtime, ZIO, ZIOAppDefault, ZLayer}

object main extends ZIOAppDefault {

  override val bootstrap: ZLayer[Any, Nothing, Unit] = Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  val appLayer: ZIO[StreamRunnerService, Throwable, Unit] = for
    _            <- zlog("Application starting")
    streamRunner <- ZIO.service[StreamRunnerService]
    _            <- streamRunner.run
  yield ()
  
  private lazy val streamRunner = appLayer.provide(
    GenericStreamRunnerService.layer,
    GenericGraphBuilderFactory.composedLayer,
    DisposeBatchProcessor.layer,
    FieldFilteringTransformer.layer,
    MergeBatchProcessor.layer,
    StagingProcessor.layer,
    FieldsFilteringService.layer,
    MyPluginStreamContext.layer,
    PosixStreamLifetimeService.layer,
    MyPluginDataProvider.layer, // Plugin implementation of a SourceDataProvider
    IcebergS3CatalogWriter.layer,
    IcebergEntityManager.sinkLayer,
    IcebergEntityManager.stagingLayer,
    IcebergTablePropertyManager.stagingLayer,
    IcebergTablePropertyManager.sinkLayer,
    JdbcMergeServiceClient.layer,
    MyPluginStreamingDataProvider.layer, // Plugin implementation of a StreamingDataProvider 
    MyPluginHookManager.layer, // Plugin implementation of a HookManager
    BackfillApplyBatchProcessor.layer,
    GenericBackfillStreamingOverwriteDataProvider.layer,
    GenericBackfillStreamingMergeDataProvider.layer,
    GenericStreamingGraphBuilder.backfillSubStreamLayer,
    MyPluginBackfillOverwriteBatchFactory.layer, // Plugin implementation of a BackfillOverwriteBatchFactory
    DeclaredMetrics.layer,
    WatermarkProcessor.layer,
    BackfillOverwriteWatermarkProcessor.layer,
    DefaultStreamBootstrapper.layer,
    ThroughputShaperBuilder.layer,
    ArcaneDimensionsProvider.layer,
    DefaultJvmMetrics.liveV2.unit
  )

  @main
  def run: ZIO[Any, Throwable, Unit] =
    val app = streamRunner

    app.catchAllCause { cause =>
      for {
        _ <- zlog(s"Application failed: ${cause.squashTrace.getMessage}", cause)
        _ <- exit(zio.ExitCode(1))
      } yield ()
    }
}

```