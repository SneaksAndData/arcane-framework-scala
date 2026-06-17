package com.sneaksanddata.arcane.framework
package services.metrics

import zio.metrics.Metric.{Counter, Gauge}
import zio.metrics.{Metric, MetricLabel}
import zio.{Task, ZIO, ZLayer}

import scala.collection.SortedMap

/** A object that contains the declared metrics names.
  */
class DeclaredMetrics(globalTags: SortedMap[String, String]):

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
    .gauge(s"$metricsNamespace.batch.stage_duration")

  /** Time it takes to merge a staging table into target
    */
  val batchMergeDuration: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.batch.merge_duration")

  /** Time it takes to dispose of a staging batch table
    */
  val batchDisposeDuration: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.batch.dispose_duration")

  /** Time it takes to commit a source shard table into the intermediate target
    */
  val shardCommitDuration: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.backfill.shard.commit_duration")

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

  val watermarkAge: Gauge[Double] = Metric
    .gauge(s"$metricsNamespace.watermark.age")

  val watermarkUpdateCounter: Counter[Long] = Metric
    .counter(s"$metricsNamespace.watermark.updates")

  val backfillStagedShards: Counter[Long] = Metric
    .counter(s"$metricsNamespace.backfill.shards_staged")

  val backfillCombinedShards: Counter[Long] = Metric
    .counter(s"$metricsNamespace.backfill.shards_combined")

  def reportMetric[Type, In, Out](metric: Metric[Type, In, Out], value: In): Task[Unit] =
    for _ <- ZIO.succeed(value) @@ metric.tagged(globalTags.map { case (key, value) =>
        MetricLabel(key, value)
      }.toSet)
    yield ()

object DeclaredMetrics:

  /** Creates a new instance of the DeclaredMetrics.
    */
  def apply(): DeclaredMetrics = new DeclaredMetrics(SortedMap.empty)

  def apply(globalTags: SortedMap[String, String]): DeclaredMetrics = new DeclaredMetrics(globalTags)

  /** The ZLayer that creates the DeclaredMetrics.
    */
  val layer: ZLayer[GlobalMetricTagProvider, Nothing, DeclaredMetrics] = ZLayer {
    for tagProvider <- ZIO.service[GlobalMetricTagProvider]
    yield DeclaredMetrics(tagProvider.getTags)
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
