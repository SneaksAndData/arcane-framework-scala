package com.sneaksanddata.arcane.framework
package services.metrics

import extensions.StringExtensions.camelCaseToSnakeCase
import models.app.PluginStreamContext
import models.settings.observability.ObservabilitySettings
import services.metrics.base.MetricTagProvider

import zio.metrics.MetricLabel
import zio.{ZIO, ZLayer}

import scala.collection.immutable.SortedMap

/** A tag provider that provides metric tags for all metrics emitted from Arcane streams.
  */
class GlobalMetricTagProvider(
    streamKind: String,
    isBackfilling: Boolean,
    streamId: String,
    backfillId: Option[String],
    observabilitySettings: ObservabilitySettings
) extends MetricTagProvider:
  private val metricPrefix = "arcane.sneaksanddata.com"

  /** Provides the metrics dimensions.
    *
    * @return
    *   The dimensions.
    */
  def getTags: SortedMap[String, String] = SortedMap(
    s"$metricPrefix/kind"      -> streamKind.camelCaseToSnakeCase,
    s"$metricPrefix/mode"      -> getStreamMode(isBackfilling),
    s"$metricPrefix/stream_id" -> streamId
  ) ++ observabilitySettings.metricTags.map { case (tagKey, tagValue) =>
    s"$metricPrefix/$tagKey" -> tagValue
  } ++ backfillId.map(id => SortedMap(s"$metricPrefix/backfill_id" -> id)).map(SortedMap.empty)

  private def getStreamMode(isBackfilling: Boolean): String = if isBackfilling then "backfill" else "stream"

/** The companion object for the DefaultMetricTagProvider class.
  */
object GlobalMetricTagProvider:

  extension (labels: Map[String, String])
    private def toMetricsLabelSet: Set[MetricLabel] =
      labels.map { case (key, value) => MetricLabel(key, value) }.toSet

  /** Creates a new instance of the DefaultMetricTagProvider.
    * @return
    *   The DefaultMetricTagProvider instance.
    */
  def apply(
      streamKind: String,
      isBackfilling: Boolean,
      streamId: String,
      backfillId: Option[String],
      observabilitySettings: ObservabilitySettings
  ): GlobalMetricTagProvider =
    new GlobalMetricTagProvider(streamKind, isBackfilling, streamId, backfillId, observabilitySettings)

  /** The ZLayer that creates the MetricTagProvider Instance.
    */
  val layer =
    ZLayer {
      for
        context       <- ZIO.service[PluginStreamContext]
        kind          <- context.streamKind.orDie
        streamId      <- context.streamId.orDie
        isBackfilling <- context.isBackfilling.orElseSucceed(false)
        backfillId    <- ZIO.when(isBackfilling)(context.backfillId)
      yield GlobalMetricTagProvider(kind, isBackfilling, streamId, backfillId, context.observability)
    }
