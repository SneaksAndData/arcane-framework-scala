package com.sneaksanddata.arcane.framework
package services.metrics

import zio.metrics.Metric.{Counter, Gauge}
import zio.metrics.{Metric, MetricLabel}
import zio.{Task, ZIO, ZLayer}

/** A object that contains the declared metrics names.
  */
class DeclaredMetrics:

  /** The namespace for the metrics. Prefixes the metric name with the namespace. For now, it is not configurable.
    */
  private val metricsNamespace = "arcane.stream"

  /** Number of rows received from the source
    */
  val rowsIncoming: Counter[Long] = Metric
    .counter(s"$metricsNamespace.rows.incoming")

  /** Chunk size for ZStream set for the next changeset, in elements
    */
  val rowChunkSize: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.rows.chunk_size")

  /** Estimated chunk size for ZStream set for the next changeset, in bytes
    */
  val rowChunkSizeBytes: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.rows.chunk_size_bytes")

  /** Estimated row chunk cost for throttle algorithm
    */
  val rowChunkCost: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.rows.chunk_cost")

  /** Memory bound shaper - estimated garbage collection frequency
    */
  val mbsGCFrequency: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.mbs.gc_frequency")

  /** Memory bound shaper - estimated garbage collection probability
    */
  val mbsGCProbability: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.mbs.gc_probability")

  /** Time it takes to transform a source rows into a mergeable batch
    */
  val batchTransformDuration: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.batch.grouping_duration")

  /** Time it takes to create a staging table from incoming row chunk
    */
  val batchStageDuration: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.batch_set.stage_duration")

  /** Time it takes to merge a staging table into target
    */
  val batchMergeDuration: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.batch.merge_duration")

  /** Time it takes to complete merge stage, which includes schema migration, merge and maintenance
    */
  val batchMergeStageDuration: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.batch_set.merge_duration")

  /** Time it takes to dispose of a staging batch table
    */
  val batchDisposeDuration: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.batch.dispose_duration")

  /** Time it takes to run optimize on the target
    */
  val targetOptimizeDuration: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.target.optimize_duration")

  /** Time it takes to run expire snapshots on the target
    */
  val targetSnapshotExpireDuration: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.target.snapshot_expire_duration")

  /** Time it takes to run orphan files removal on the target
    */
  val targetRemoveOrphanDuration: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.target.remove_orphan_duration")

  /** Time it takes to run ANALYZE on the target
    */
  val targetAnalyzeDuration: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.target.analyze_duration")

  val appliedWatermarkAge: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.watermark.applied_age")

  val streamingWatermarkAge: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.watermark.streaming_age")

object DeclaredMetrics:

  /** Creates a new instance of the DeclaredMetrics.
    *
    * @return
    *   The ArcaneDimensionsProvider instance.
    */
  def apply(): DeclaredMetrics = new DeclaredMetrics()

  /** The ZLayer that creates the DeclaredMetrics.
    */
  val layer: ZLayer[Any, Nothing, DeclaredMetrics] = ZLayer.succeed(DeclaredMetrics())

  /** Measures running time of each task
    */
  extension [TaskResult](task: Task[TaskResult])
    def gaugeDuration(metric: Gauge[Double]): ZIO[Any, Throwable, TaskResult] = for
      startTime <- ZIO.succeed(System.nanoTime())
      result    <- task
      endTime   <- ZIO.succeed(System.nanoTime())
      _         <- ZIO.succeed((endTime - startTime).toDouble) @@ metric
    yield result
