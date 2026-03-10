package com.sneaksanddata.arcane.framework
package services.metrics

import extensions.StringExtensions.camelCaseToSnakeCase
import models.app.{BaseStreamContext, PluginStreamContext}
import models.settings.observability.ObservabilitySettings
import services.base.DimensionsProvider

import zio.{ZIO, ZLayer}

import scala.collection.immutable.SortedMap

/** A dimensions provider that provides dimensions for DataDog metrics service.
  */
class ArcaneDimensionsProvider(
    streamKind: String,
    isBackfilling: Boolean,
    streamId: String,
    observabilitySettings: ObservabilitySettings
) extends DimensionsProvider:
  private val dimensionPrefix = "arcane.sneaksanddata.com"

  /** Provides the metrics dimensions.
    *
    * @return
    *   The dimensions.
    */
  def getDimensions: SortedMap[String, String] = SortedMap(
    s"$dimensionPrefix/kind"      -> streamKind.camelCaseToSnakeCase,
    s"$dimensionPrefix/mode"      -> getStreamMode(isBackfilling),
    s"$dimensionPrefix/stream_id" -> streamId
  ) ++ observabilitySettings.metricTags.map { case (tagKey, tagValue) =>
    s"$dimensionPrefix/$tagKey" -> tagValue
  }

  private def getStreamMode(isBackfilling: Boolean): String = if isBackfilling then "backfill" else "stream"

/** The companion object for the ArcaneDimensionsProvider class.
  */
object ArcaneDimensionsProvider:
  /** The environment type for the ArcaneDimensionsProvider.
    */
  type Environment = PluginStreamContext

  /** Creates a new instance of the ArcaneDimensionsProvider.
    * @return
    *   The ArcaneDimensionsProvider instance.
    */
  def apply(
      streamKind: String,
      isBackfilling: Boolean,
      streamId: String,
      observabilitySettings: ObservabilitySettings
  ): ArcaneDimensionsProvider =
    new ArcaneDimensionsProvider(streamKind, isBackfilling, streamId, observabilitySettings)

  /** The ZLayer that creates the ArcaneDimensionsProvider.
    */
  val layer: ZLayer[Environment, Nothing, DimensionsProvider] =
    ZLayer {
      for context <- ZIO.service[PluginStreamContext]
      yield ArcaneDimensionsProvider(context.streamKind, context.isBackfilling, context.streamId, context.observability)
    }
