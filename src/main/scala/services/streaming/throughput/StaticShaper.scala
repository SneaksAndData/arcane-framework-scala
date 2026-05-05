package com.sneaksanddata.arcane.framework
package services.streaming.throughput

import models.settings.FlowRate
import models.settings.streaming.ThroughputSettings
import services.streaming.throughput.base.ThroughputShaper

import zio.{Chunk, Task, ZIO}

/** Simple shaper that applies static values from the configuration. Chunk cost is set to 1 for all chunks.
  * @param throughputSettings
  *   Fixed throughput settings
  */
class StaticShaper(throughputSettings: ThroughputSettings) extends ThroughputShaper:
  override def estimateChunkSize: Task[(Elements: Int, ElementSize: Long)] =
    ZIO.succeed((throughputSettings.advisedChunkSize, 1))

  override def estimateShapeBurst(chunkSize: Int, chunkElementSize: Long): Task[Int] =
    ZIO.succeed(throughputSettings.advisedBurst)

  override def estimateShapeRate(chunkSize: Int, chunkElementSize: Long): Task[FlowRate] =
    ZIO.succeed(throughputSettings.advisedRate)

  override def estimateChunkCost[Element](ch: Chunk[Element]): Int = 1
