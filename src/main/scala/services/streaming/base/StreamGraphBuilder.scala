package com.sneaksanddata.arcane.framework
package services.streaming.base

import zio.stream.{ZSink, ZStream}
import zio.{Task, ZIO}

/**
 * A trait that represents a stream graph builder.
 */
trait StreamGraphBuilder:

  /**
   * The type of the stream element produced by the source and consumed by the sink.
   */
  type StreamElementType

  /**
   * Creates a ZStream for the stream graph.
   *
   * @return ZStream (stream source for the stream graph).
   */
  def create: ZStream[Any, Throwable, StreamElementType]

  /**
   * Creates a ZStream for the stream graph.
   *
   * @return ZStream (stream source for the stream graph).
   */
  def consume: ZSink[Any, Throwable, StreamElementType, Any, Unit]
  
  /**
   * The action to perform when the stream completes.
   *
   * @return A task that represents the action to perform when the stream completes.
   */
  def onComplete: Task[Unit] = ZIO.unit