package com.sneaksanddata.arcane.framework
package services.streaming.throughput.base

import logging.ZIOLogAnnotations.zlog
import models.settings.FlowRate

import zio.stream.ZStream
import zio.{Chunk, Task}

import java.time.Duration

trait ThroughputShaper:

  def estimateChunkSize: Task[(Elements: Int, ElementSize: Long)]

  def estimateShapeBurst(chunkSize: Int, chunkElementSize: Long): Task[Int]

  def estimateShapeRate(chunkSize: Int, chunkElementSize: Long): Task[FlowRate]

  def estimateChunkCost[Element](ch: Chunk[Element]): Int

  def shapeStream[Element](stream: ZStream[Any, Throwable, Element]): ZStream[Any, Throwable, Element] =
    ZStream
      .fromZIO {
        for
          chunkSize <- estimateChunkSize
          burst     <- estimateShapeBurst(chunkSize.Elements, chunkSize.ElementSize)
          rate      <- estimateShapeRate(chunkSize.Elements, chunkSize.ElementSize)
          _ <- zlog(
            "Shaping stream using chunkSize %s, burst %s and rate %s/%s (elements/s)",
            chunkSize.Elements.toString,
            burst.toString,
            rate.elements.toString,
            rate.interval.toSeconds.toString
          )
        yield (Size = chunkSize, Burst = burst, Rate = rate)
      }
      .flatMap { case (size, burst, rate) =>
        stream.rechunk(1).throttleShape(rate.elements, rate.interval, burst)(estimateChunkCost).rechunk(size.Elements)
      }
