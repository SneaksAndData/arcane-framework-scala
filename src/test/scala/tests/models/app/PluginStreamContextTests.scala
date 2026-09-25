package com.sneaksanddata.arcane.framework
package tests.models.app

import models.app.{DefaultOverrideStreamContext, OverrideStreamContext, PluginStreamContext}
import models.settings.sources.OverrideStreamSourceSettings
import tests.shared.TestPluginStreamContextImpl

import upickle.default.*
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.statsd.DatagramSocketConfig
import zio.test.*
import zio.{Scope, ZIO}

case class GetLayerTestContext(marker: String) extends TestPluginStreamContextImpl derives ReadWriter:
  override def merge[OtherImpl <: OverrideStreamContext](other: Option[OtherImpl]): this.type = this

case class GetLayerTestOverrides() extends DefaultOverrideStreamContext derives ReadWriter:
  override val source: Option[OverrideStreamSourceSettings] = None

object PluginStreamContextTests extends ZIOSpecDefault:
  private val layer = PluginStreamContext.getLayer[GetLayerTestContext, GetLayerTestOverrides]

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("PluginStreamContext.getLayer")(
    test("provides the plugin context and derived metrics configs") {
      for
        _             <- TestSystem.putEnv("STREAMCONTEXT__SPEC", """{"marker":"from-env"}""")
        context       <- ZIO.service[PluginStreamContext].provideLayer(layer)
        socketConfig  <- ZIO.service[DatagramSocketConfig].provideLayer(layer)
        metricsConfig <- ZIO.service[MetricsConfig].provideLayer(layer)
      yield assertTrue(
        context.asInstanceOf[GetLayerTestContext].marker == "from-env",
        socketConfig == DatagramSocketConfig(context.datadogSocketPath),
        metricsConfig == MetricsConfig(context.metricsPublisherInterval)
      )
    },
    test("fails when STREAMCONTEXT__SPEC is not defined") {
      for result <- ZIO.service[PluginStreamContext].provideLayer(layer).exit
      yield assertTrue(result.isFailure)
    }
  )
