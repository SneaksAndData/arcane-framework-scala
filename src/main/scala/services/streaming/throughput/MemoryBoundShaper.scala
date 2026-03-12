package com.sneaksanddata.arcane.framework
package services.streaming.throughput

import logging.ZIOLogAnnotations.zlog
import models.settings.sink.SinkSettings
import models.settings.streaming.{MemoryBound, MemoryBoundImpl, ThroughputSettings}
import services.iceberg.base.SinkPropertyManager
import services.metrics.DeclaredMetrics
import services.streaming.throughput.base.ThroughputShaper

import org.apache.iceberg.Schema
import org.apache.iceberg.types.Type.TypeID
import zio.{Chunk, Task, ZIO}

import java.time.Duration
import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters.*
import scala.math.{exp, log}

/** Throughput shaper the uses information from Java Runtime on available memory to limit stream throughput. This shaper
  * ensures application doesn't crash with OOM on bursts, but results in slower stream chunk processing compared to less
  * conservative shapers. Chunk cost and amount of memory available for allocation are estimated using sigmoid
  * functions, based on target table size in bytes and rows.
  *
  * For partitioned tables, this shaper will force chunk size to be smaller than number of partitions in the table, thus
  * providing merge speed increase.
  */
class MemoryBoundShaper(
    tablePropertyManager: SinkPropertyManager,
    targetTableShortName: String,
    throughputSettings: ThroughputSettings,
    declaredMetrics: DeclaredMetrics
) extends ThroughputShaper:

  private val shaperSettings = throughputSettings.shaperImpl match
    case mb: MemoryBoundImpl => mb.memoryBound
    case _ =>
      throw new RuntimeException("`shaperImpl.$$type` must be set to `MemoryBound` when using MemoryBoundShaper")

  private val runtime            = Runtime.getRuntime
  private val maxAvailableMemory = runtime.maxMemory()
  private val mib                = 1024 * 1024
  private val estimationCache    = TrieMap[String, Double]()

  private val rowSizeCacheKey = "rowsize"
  private val memCacheKey     = "memcutoff"
  private val partsCacheKey   = "partitions"

  private def getUsedMemoryShare = (maxAvailableMemory - runtime.freeMemory()) / maxAvailableMemory.toDouble

  /** Estimate memory pool available for chunks. Larger tables get larger pool to allow bigger chunks
    */
  private def estimateMemoryCutoff(estRows: Long, estSize: Long): Double = if estRows * estSize == 0
  then 0.5
  else
    scaledSigmoid(
      shaperSettings.chunkCostMax,
      shaperSettings.tableRowCountWeight * log(estRows) + shaperSettings.tableSizeWeight * log(estSize),
      shaperSettings.tableSizeScaleFactor
    )

  private def estimateRowSize(schema: Schema): Long =
    schema.columns().asScala.map(_.`type`()).foldLeft(0L) { case (agg, tp) =>
      tp.typeId() match
        // 8L added to each type to hold pointer, since all types are objects
        case TypeID.TIME    => 4L + 8L
        case TypeID.INTEGER => 4L + 8L
        case TypeID.BOOLEAN => 1L + 8L
        case TypeID.LONG    => 8L + 8L
        case TypeID.FLOAT   => 4L + 8L
        case TypeID.DOUBLE  => 8L + 8L
        case TypeID.STRING =>
          2L * shaperSettings.meanStringTypeSizeEstimate.toLong + 8L // conservative over-estimation for varchar types
        case TypeID.DECIMAL        => 8L + 8L + 8L
        case TypeID.TIMESTAMP      => 8L + 8L
        case TypeID.TIMESTAMP_NANO => 8L + 8L
        case _ =>
          8L + shaperSettings.meanObjectTypeSizeEstimate.toLong // assume large size for structs, lists, geometry, variant and other less common types
    }

  override def estimateChunkSize: Task[(Elements: Int, ElementSize: Long)] = for
    _ <- zlog("Estimating chunk size for the stream")
    _ <- ZIO.when(estimationCache.isEmpty) {
      for
        tableSizeEstimate   <- tablePropertyManager.getTableSize(targetTableShortName)
        tablePartitionCount <- tablePropertyManager.getPartitionCount(targetTableShortName)
        tableSchema         <- tablePropertyManager.getTableSchema(targetTableShortName)

        memoryCutoff <- ZIO.succeed(estimateMemoryCutoff(tableSizeEstimate.Records, tableSizeEstimate.Size))
        rowsSize <- ZIO.succeed(
          Seq(
            estimateRowSize(tableSchema).toDouble,
            tableSizeEstimate.Records / (tableSizeEstimate.Size.toDouble + 1)
          ).max
        )
        _ <- ZIO.succeed(estimationCache.addOne((memCacheKey, memoryCutoff)))
        _ <- ZIO.succeed(estimationCache.addOne((rowSizeCacheKey, rowsSize)))
        _ <- ZIO.succeed(estimationCache.addOne((partsCacheKey, tablePartitionCount.toDouble)))
        _ <- zlog(
          "Computed baseline estimation parameters: memory cutoff %s and row size %s",
          memoryCutoff.toString,
          rowsSize.toString
        )
      yield ()
    }

    chunkSizeFromRowSize <- ZIO.succeed(
      runtime.freeMemory() * estimationCache(memCacheKey) / (estimationCache(rowSizeCacheKey) + 1)
    )
    _ <- zlog("Estimated chunk size %s for the current stream", chunkSizeFromRowSize.toInt.toString)
    appliedSize <- ZIO.succeed(
      if estimationCache("partitions").toInt > 1 then
        (
          Seq(chunkSizeFromRowSize, estimationCache("partitions") / 2).min.toInt,
          estimationCache(rowSizeCacheKey).toLong
        )
      else
        (
          Seq(chunkSizeFromRowSize, 1.0).max.toInt,
          estimationCache(rowSizeCacheKey).toLong
        )
    )
    estMemoryPerChunk <- ZIO.succeed(appliedSize._1 * appliedSize._2 / mib)
    _ <- zlog(
      "Will apply chunk size %s for the current stream, estimated memory request %s MiB",
      appliedSize._1.toString,
      estMemoryPerChunk.toString
    )
    _ <- ZIO.succeed(appliedSize._1.toDouble) @@ declaredMetrics.rowChunkSize
    _ <- ZIO.succeed(estMemoryPerChunk.toDouble) @@ declaredMetrics.rowChunkSizeBytes
    _ <- ZIO.succeed(estimateChunkCost(appliedSize._1).toDouble) @@ declaredMetrics.rowChunkCost
  yield appliedSize

  override def estimateShapeBurst(chunkSize: Int, chunkElementSize: Long): Task[Int] =
    for chunksToFit <- ZIO.attempt(runtime.maxMemory() / (chunkSize * chunkElementSize + 1))
    yield Seq(
      chunksToFit.toDouble / shaperSettings.burstEstimateDivisionFactor,
      throughputSettings.advisedChunksBurst.toDouble
    ).max.toInt

  override def estimateShapeRate(chunkSize: Int, chunkElementSize: Long): Task[(Elements: Int, Period: Duration)] =
    for chunksToFit <- ZIO.attempt(runtime.maxMemory() / (chunkSize * chunkElementSize + 1))
    yield (
      Seq(
        chunksToFit.toDouble / shaperSettings.rateEstimateDivisionFactor,
        throughputSettings.advisedRateChunks.toDouble
      ).max.toInt,
      throughputSettings.advisedRatePeriod
    )

    /** Project (-inf, inf) to (0, maxBound) https://en.wikipedia.org/wiki/Sigmoid_function factor for range projection
      * Higher values increase sensitivity near 0. Midpoint is shifted as our value is always greater than 0
      */
  private def scaledSigmoid(maxBound: Double, value: Double, k: Int): Double =
    maxBound * (2.0 / (1.0 + exp(-1.0 * k * value)) - 1)

  override def estimateChunkCost[Element](ch: Chunk[Element]): Int = estimateChunkCost(ch.size)

  private def estimateChunkCost(size: Int): Int =
    val rawCost = size * estimationCache(rowSizeCacheKey) / (runtime.freeMemory() + 1)
    val cost    = scaledSigmoid(shaperSettings.chunkCostMax, rawCost, shaperSettings.chunkCostScale).toInt
    scaledSigmoid(shaperSettings.chunkCostMax, rawCost, shaperSettings.chunkCostScale).toInt

object MemoryBoundShaper:
  /** Factory method to create MemoryBoundShaper
    */
  def apply(
      propertyManager: SinkPropertyManager,
      targetTableShortName: String,
      memoryBoundShaperSettings: ThroughputSettings,
      declaredMetrics: DeclaredMetrics
  ): MemoryBoundShaper =
    new MemoryBoundShaper(propertyManager, targetTableShortName, memoryBoundShaperSettings, declaredMetrics)
