package com.sneaksanddata.arcane.framework
package services.streaming.throughput

import services.streaming.throughput.base.ThroughputShaper

import zio.{Chunk, Task, ZIO}

import java.time.Duration

class VoidShaper extends ThroughputShaper:
  override def estimateChunkSize: Task[(Elements: Int, ElementSize: Long)] = ZIO.succeed((1000, 1))

  override def estimateShapeBurst(chunkSize: Int, chunkElementSize: Long): Task[Int] = ZIO.succeed(1000)

  override def estimateShapeRate(chunkSize: Int, chunkElementSize: Long): Task[(Elements: Int, Period: Duration)] =
    ZIO.succeed((1, Duration.ofSeconds(1)))

  override def estimateChunkCost[Element](ch: Chunk[Element]): Int = 1
