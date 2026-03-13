# arcane-framework-scala
Arcane — A simple ZIO-based data streaming framework implemented in Scala

## Creating plugins
Plugins utilize framework to create specific source-to-Iceberg streaming applications.
Developers should create a Scala3 project for each plugin, containing stream context definition, source settings definition and `main.scala`.

### Stream context definition
Most plugins can rely on framework defaults:
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