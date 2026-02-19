package com.sneaksanddata.arcane.framework
package services.streaming.throughput.base

import zio.{Chunk, Task}
import zio.stream.ZStream

import java.time.Duration

trait ThroughputShaper:
  
  def estimateChunkSize: Task[(Elements: Int, ElementSize: Long)]
  
  def estimateShapeBurst(chunkSize: Int, chunkElementSize: Long): Task[Int]
  
  def estimateShapeRate(chunkSize: Int, chunkElementSize: Long): Task[(Elements: Int, Period: Duration)]

  def estimateChunkCost[Element](ch: Chunk[Element]): Int
  
  def shapeStream[Element](stream: ZStream[Any, Throwable, Element]): ZStream[Any, Throwable, Element] =
    ZStream.fromZIO {
      for
        chunkSize <- estimateChunkSize
        burst <- estimateShapeBurst(chunkSize.Elements, chunkSize.ElementSize)
        rate <- estimateShapeRate(chunkSize.Elements, chunkSize.ElementSize)
      yield (Size = chunkSize, Burst = burst, Rate = rate) 
    }.flatMap {
      case (size, burst, rate) => stream.rechunk(size.Elements).throttleShape(rate.Elements, rate.Period, burst)(estimateChunkCost)
    }
