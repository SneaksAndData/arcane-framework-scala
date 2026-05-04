package com.sneaksanddata.arcane.framework
package services.streaming.throughput

import logging.ZIOLogAnnotations.zlog
import models.settings.FlowRate
import models.settings.streaming.{MemoryBoundImpl, ThroughputSettings}
import services.iceberg.base.SinkPropertyManager
import services.metrics.DeclaredMetrics
import services.streaming.throughput.base.ThroughputShaper

import org.apache.iceberg.Schema
import org.apache.iceberg.types.Type.TypeID
import zio.{Chunk, Task, ZIO}

import java.lang.management.ManagementFactory
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
      throw new RuntimeException("`shaperImpl` must be set to `memoryBound` when using MemoryBoundShaper")

  private val runtime            = Runtime.getRuntime
  private val maxAvailableMemory = runtime.maxMemory()
  private val mib                = 1024 * 1024
  private val estimationCache    = TrieMap[String, Double]()

  private val rowSizeCacheKey    = "rowsize"
  private val memCacheKey        = "memcutoff"
  private val partsCacheKey      = "partitions"
  private val stringSizeCacheKey = "stringsize"

  private def getTotalFreeMemory =
    val allocatedTotal = runtime.totalMemory()
    val freeOutOfTotal = runtime.freeMemory()

    // unallocated + free in the allocated
    maxAvailableMemory - allocatedTotal + freeOutOfTotal

  /** Estimate memory pool available for chunks. Larger tables get larger pool to allow bigger chunks
    */
  private def estimateMemoryCutoff(estRows: Long, estSize: Long): Double = if estRows * estSize == 0
  then 0.5
  else
    scaledSigmoid(
      0.8,
      shaperSettings.tableRowCountWeight * log(estRows) + shaperSettings.tableSizeWeight * log(estSize),
      shaperSettings.tableSizeScaleFactor
    )

  /** Estimate string length in the file. Sum uncompressed size of all string fields, divide by record count -> avg
    * field size in bytes, divide by 2L to get length
    * @return
    */
  private def estimateStringLength(schema: Schema, sizes: Map[Int, Long], recordCount: Long): Long =
    if sizes.isEmpty || recordCount == 0L then shaperSettings.fallbackStringTypeSizeEstimate.toLong
    else
      (schema
        .columns()
        .asScala
        .filter(_.`type`().typeId() == TypeID.STRING)
        .map(v => sizes.getOrElse(v.fieldId(), 0L))
        // find avg field size and multiply by 1.5 to reserve slightly more memory for extra safety
        .sum * 1.5 / recordCount / 2L).toLong

  private def estimateRowSize(schema: Schema, estimatedStringLength: Long): Long =
    schema.columns().asScala.map(_.`type`()).foldLeft(0L) { case (agg, tp) =>
      val typeSize = tp.typeId() match
        // 8L added to each type to hold pointer, since all types are objects
        // 16L for Java object header, ignore COH to be on the safe side
        // add 4L for padding for all except boolean
        case TypeID.TIME =>
          4L      // data
            + 8L  // pointer
            + 16L // header
            + 4L  // padding
        case TypeID.INTEGER =>
          4L      // data
            + 8L  // pointer
            + 16L // header
            + 4L  // padding
        case TypeID.BOOLEAN =>
          1L      // data
            + 8L  // pointer
            + 16L // header
            + 11L // padding
        case TypeID.LONG =>
          8L      // data
            + 8L  // pointer
            + 16L // header
            + 4L  // padding
        case TypeID.FLOAT =>
          4L      // data
            + 8L  // pointer
            + 16L // header
            + 4L  // padding
        case TypeID.DOUBLE =>
          8L      // data
            + 8L  // pointer
            + 16L // header
            + 4L  // padding
        case TypeID.STRING =>
          32L     // wrapper type
            + 16L // array header
            + 2L * estimatedStringLength
        case TypeID.DECIMAL =>
          16L         // header
            + 8L      // bigint pointer
            + 4L + 4L // scale and precision
            + 16L     // bingint wrapper header
            + 8L      // array pointer
            + 4L + 4L // sign and length
            + 16L     // extra metadata
            + 32L     // data array
        case TypeID.TIMESTAMP =>
          8L      // data
            + 8L  // pointer
            + 16L // header
            + 4L  // padding
        case TypeID.TIMESTAMP_NANO =>
          8L      // data
            + 8L  // pointer
            + 16L // header
            + 4L  // padding
        case _ =>
          16L + 4L + 8L + shaperSettings.objectTypeSizeEstimate.toLong // assume large size for structs, lists, geometry, variant and other less common types

      agg + typeSize
    }

  override def estimateChunkSize: Task[(Elements: Int, ElementSize: Long)] = for
    _ <- zlog("Estimating chunk size for the stream")
    _ <- ZIO.when(estimationCache.isEmpty) {
      for
        tableSizeEstimate <- tablePropertyManager
          .getTableSize(targetTableShortName)
        tablePartitionCount <- tablePropertyManager.getPartitionCount(targetTableShortName)
        tableSchema         <- tablePropertyManager.getTableSchema(targetTableShortName)
        stringSize <- tablePropertyManager
          .getColumnSizes(targetTableShortName)
          .map(v => estimateStringLength(tableSchema, v, tableSizeEstimate.Records))

        memoryCutoff <- ZIO.succeed(estimateMemoryCutoff(tableSizeEstimate.Records, tableSizeEstimate.Size))
        rowsSize <- ZIO.succeed(
          Seq(
            estimateRowSize(tableSchema, stringSize).toDouble,
            tableSizeEstimate.Records / (tableSizeEstimate.Size.toDouble + 1)
          ).max
        )
        _ <- ZIO.succeed(estimationCache.addOne((memCacheKey, memoryCutoff)))
        _ <- ZIO.succeed(estimationCache.addOne((rowSizeCacheKey, rowsSize)))
        _ <- ZIO.succeed(estimationCache.addOne((partsCacheKey, tablePartitionCount.toDouble)))
        _ <- ZIO.succeed(estimationCache.addOne((stringSizeCacheKey, stringSize.toDouble)))
        _ <- zlog(
          "Computed baseline estimation parameters: memory cutoff %s, row size %s (bytes), string field size %s (characters)",
          memoryCutoff.toString,
          rowsSize.toString,
          stringSize.toString
        )
      yield ()
    }

    chunkSizeFromRowSize <- ZIO.succeed(
      getTotalFreeMemory * estimationCache(memCacheKey) / (estimationCache(
        rowSizeCacheKey
      ) + 1) / 2 // estimate for 2 chunks in memory at all times
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
    for rowsToFit <- ZIO.attempt(getTotalFreeMemory / (chunkElementSize + 1))
    yield Seq(
      rowsToFit.toDouble,
      0.1 * chunkSize,
      throughputSettings.advisedBurst.toDouble
    ).max.toInt

  private def getTotalGCCount: Long =
    ManagementFactory.getGarbageCollectorMXBeans.asScala.foldLeft(0L) { case (_, bean) =>
      val gcs = bean.getCollectionCount
      if gcs >= 0 then gcs
      else 0
    }

  private def getUptime: Long = ManagementFactory.getRuntimeMXBean.getUptime / 1000

  override def estimateShapeRate(chunkSize: Int, chunkElementSize: Long): Task[FlowRate] = {
    // utilize leaking bucket model for memory
    for
      // assume GC "leaks" out of the memory bucket with certain probability
      // assume GC frees at least one chunk out
      currentUptime <- ZIO.succeed(getUptime)
      gcFrequency   <- ZIO.attempt((getTotalGCCount.toDouble + 1.0) / currentUptime.toDouble)
      gcProbability <- ZIO.succeed(
        1 - Math.exp(
          -1 * gcFrequency * Seq(1, currentUptime.toDouble / throughputSettings.advisedRate.interval.toSeconds).min
        )
      ) // in relation to uptime!

      // report metrics so calculations can be traced
      _ <- ZIO.succeed(gcFrequency) @@ declaredMetrics.mbsGCFrequency
      _ <- ZIO.succeed(gcProbability) @@ declaredMetrics.mbsGCProbability

    // add 1 + hold 1: chunkSize + chunkSize
    // leak: chunkSize * gcProbability
    yield FlowRate(
      elements = ((chunkSize * (1 + gcProbability)) / throughputSettings.advisedRate.interval.toSeconds).toInt + 1,
      interval = Duration.ofSeconds(1)
    )
  }

  /** Project (-inf, inf) to (0, maxBound) https://en.wikipedia.org/wiki/Sigmoid_function factor for range projection
    * Higher values increase sensitivity near 0. Midpoint is shifted as our value is always greater than 0
    */
  private def scaledSigmoid(maxBound: Double, value: Double, k: Int): Double =
    maxBound * (2.0 / (1.0 + exp(-1.0 * k * value)) - 1)

  override def estimateChunkCost[Element](ch: Chunk[Element]): Int = estimateChunkCost(ch.size)

  private def estimateChunkCost(size: Int): Int =
    val rawCost = 2 * size * estimationCache(rowSizeCacheKey) / (getTotalFreeMemory + 1)
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
