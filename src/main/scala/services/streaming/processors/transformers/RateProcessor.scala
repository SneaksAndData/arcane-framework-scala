package com.sneaksanddata.arcane.framework
package services.streaming.processors.transformers

import services.streaming.base.RowProcessor

import zio.Duration
import zio.stream.ZPipeline


class RateProcessor extends RowProcessor:
  private val runtime = Runtime.getRuntime
  private val maxAvailableMemory = runtime.maxMemory()

  private def getUsedMemoryShare = (maxAvailableMemory - runtime.freeMemory()) / maxAvailableMemory

  override def process: ZPipeline[Any, Throwable, Element, Element] = ZPipeline
    .rechunk(10)
    .throttleShape(10, Duration.fromSeconds(10), 1)(ch => 1)