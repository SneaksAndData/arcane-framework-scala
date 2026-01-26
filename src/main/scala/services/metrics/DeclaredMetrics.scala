package com.sneaksanddata.arcane.framework
package services.metrics

import services.base.DimensionsProvider

import zio.metrics.Metric.{Counter, Gauge}
import zio.metrics.{Metric, MetricLabel}
import zio.{Task, ZIO, ZLayer}

/** A object that contains the declared metrics names.
  */
class DeclaredMetrics(dimensionsProvider: DimensionsProvider):

  /** The namespace for the metrics. Prefixes the metric name with the namespace. For now, it is not configurable.
    */
  private val metricsNamespace = "arcane.stream"

  /** Number of rows received from the source
    */
  val rowsIncoming: Counter[Long] = Metric
    .counter(s"$metricsNamespace.rows.incoming")
    .tagged(dimensionsProvider.getDimensions.toMetricsLabelSet)

  /** Time it takes to transform a source rows into a mergeable batch
    */
  val batchTransformDuration: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.batch.grouping_duration")
    .tagged(dimensionsProvider.getDimensions.toMetricsLabelSet)

  /** Time it takes to create a staging table from incoming row chunk
    */
  val batchStageDuration: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.batch_set.stage_duration")
    .tagged(dimensionsProvider.getDimensions.toMetricsLabelSet)

  /** Time it takes to merge a staging table into target
    */
  val batchMergeDuration: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.batch.merge_duration")
    .tagged(dimensionsProvider.getDimensions.toMetricsLabelSet)

  /** Time it takes to complete merge stage, which includes schema migration, merge and maintenance
    */
  val batchMergeStageDuration: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.batch_set.merge_duration")
    .tagged(dimensionsProvider.getDimensions.toMetricsLabelSet)

  /** Time it takes to dispose of a staging batch table
    */
  val batchDisposeDuration: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.batch.dispose_duration")
    .tagged(dimensionsProvider.getDimensions.toMetricsLabelSet)

  /** Time it takes to run optimize on the target
    */
  val targetOptimizeDuration: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.target.optimize_duration")
    .tagged(dimensionsProvider.getDimensions.toMetricsLabelSet)

  /** Time it takes to run expire snapshots on the target
    */
  val targetSnapshotExpireDuration: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.target.snapshot_expire_duration")
    .tagged(dimensionsProvider.getDimensions.toMetricsLabelSet)

  /** Time it takes to run orphan files removal on the target
    */
  val targetRemoveOrphanDuration: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.target.remove_orphan_duration")
    .tagged(dimensionsProvider.getDimensions.toMetricsLabelSet)

  /** Time it takes to run ANALYZE on the target
    */
  val targetAnalyzeDuration: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.target.analyze_duration")
    .tagged(dimensionsProvider.getDimensions.toMetricsLabelSet)

  val appliedWatermarkAge: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.watermark.applied_age")
    .tagged(dimensionsProvider.getDimensions.toMetricsLabelSet)

  val streamingWatermarkAge: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.watermark.streaming_age")
    .tagged(dimensionsProvider.getDimensions.toMetricsLabelSet)

  def tagMetric[Type, In, Out](metric: Metric[Type, In, Out]): Metric[Type, In, Out] =
    metric.tagged(dimensionsProvider.getDimensions.toMetricsLabelSet)

  extension (labels: Map[String, String])
    private def toMetricsLabelSet: Set[MetricLabel] =
      labels.map { case (key, value) => MetricLabel(key, value) }.toSet

object DeclaredMetrics:

  /** The environment type for the DeclaredMetrics.
    */
  type Environment = DimensionsProvider

  /** Creates a new instance of the DeclaredMetrics.
    *
    * @param dimensionsProvider
    *   The stream context.
    * @return
    *   The ArcaneDimensionsProvider instance.
    */
  def apply(dimensionsProvider: DimensionsProvider): DeclaredMetrics = new DeclaredMetrics(dimensionsProvider)

  /** The ZLayer that creates the DeclaredMetrics.
    */
  val layer: ZLayer[Environment, Nothing, DeclaredMetrics] =
    ZLayer {
      for dimensionsProvider <- ZIO.service[DimensionsProvider]
      yield DeclaredMetrics(dimensionsProvider)
    }

  /** Measures running time of each task
    */
  extension [TaskResult](task: Task[TaskResult])
    def gaugeDuration(metric: Gauge[Double]): ZIO[Any, Throwable, TaskResult] = for
      startTime <- ZIO.succeed(System.nanoTime())
      result    <- task
      endTime   <- ZIO.succeed(System.nanoTime())
      _         <- ZIO.succeed((endTime - startTime).toDouble) @@ metric
    yield result
