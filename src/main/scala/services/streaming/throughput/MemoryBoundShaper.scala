package com.sneaksanddata.arcane.framework
package services.streaming.throughput

import logging.ZIOLogAnnotations.zlog
import models.settings.sink.SinkSettings
import models.settings.streaming.ThroughputSettings
import models.settings.streaming.ThroughputShaperImpl.MemoryBound
import services.iceberg.base.TablePropertyManager
import services.streaming.throughput.base.ThroughputShaper

import org.apache.iceberg.Schema
import org.apache.iceberg.types.Type.TypeID
import zio.{Chunk, Task, ZIO, ZLayer}

import java.time.Duration
import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters.*
import scala.math.{exp, log}

class MemoryBoundShaper(
    tablePropertyManager: TablePropertyManager,
    sinkSettings: SinkSettings,
    throughputSettings: ThroughputSettings
) extends ThroughputShaper:

  private val shaperSettings = throughputSettings.shaperImpl match
    case mb: MemoryBound => mb
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
    * @param estRows
    * @param estSize
    * @return
    */
  private def estimateMemoryCutoff(estRows: Long, estSize: Long): Double = scaledSigmoid(
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
        tableSizeEstimate   <- tablePropertyManager.getTableSize(sinkSettings.targetTableNameParts.Name)
        tablePartitionCount <- tablePropertyManager.getPartitionCount(sinkSettings.targetTableNameParts.Name)
        tableSchema         <- tablePropertyManager.getTableSchema(sinkSettings.targetTableNameParts.Name)

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
    _ <- zlog(
      "Will apply chunk size %s for the current stream, estimated memory request %s MiB",
      appliedSize._1.toString,
      (appliedSize._1 * appliedSize._2 / mib).toString
    )
  // TODO: add these as metrics
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

    /** Project (-inf, inf) to (0, 1) https://en.wikipedia.org/wiki/Sigmoid_function
      *
      * @param Scaling
      *   factor for range projection Higher values increase sensitivity near 0.
      */
  private def scaledSigmoid(value: Double, k: Int): Double = 1.0 / (1.0 + exp(-1.0 * k * value))

  override def estimateChunkCost[Element](ch: Chunk[Element]): Int =
    val rawCost = ch.size * estimationCache(rowSizeCacheKey).toLong / (runtime.freeMemory() + 1)
    (scaledSigmoid(rawCost.toDouble, shaperSettings.chunkCostScale) * shaperSettings.chunkCostMax).toInt

object MemoryBoundShaper:
  private type Environment = TablePropertyManager & SinkSettings

  /** Factory method to create MemoryBoundShaper
    *
    * @param sinkSettings
    *   Sink settings
    * @return
    *   The initialized IcebergTablePropertyManager instance
    */
  def apply(
      propertyManager: TablePropertyManager,
      sinkSettings: SinkSettings,
      memoryBoundShaperSettings: ThroughputSettings
  ): MemoryBoundShaper =
    new MemoryBoundShaper(propertyManager, sinkSettings, memoryBoundShaperSettings)
