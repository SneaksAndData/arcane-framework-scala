package com.sneaksanddata.arcane.framework
package services.backfill

import services.streaming.base.StreamingGraphBuilder

import zio.stream.ZStream

/** A trait that represents a stream graph builder for backfilling.
  */
trait BackfillStreamingGraphBuilder extends StreamingGraphBuilder
