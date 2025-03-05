package com.sneaksanddata.arcane.framework
package extensions

import logging.ZIOLogAnnotations.zlog

import zio.ZIO
import zio.stream.ZStream

/**
 * Extension methods for ZStream
 */
object ZStreamExtensions:
  
  /**
   * The shorthand method to produce the stream that contains all elements from the input stream except the last one.
   * Works even if the stream with unknown size.
   * NOTE: the stream is fully consumed to calculate the size. This behavior is not optimal for infinite streams and will be improved in the future.
   * @return The transformed stream.
   */
  extension[E, A] (stream: ZStream[Any, E, A]) def dropLast: ZStream[Any, E, A] =
    val task = for stream <- ZIO.succeed(stream)
        items <- stream.runCollect
        _ <- zlog(s"Dropping last element from from the blobs stream: ${if items.nonEmpty then items.last.toString else "(empty)"}")
    yield if items.nonEmpty then items.dropRight(1) else items
    
    ZStream.fromZIO(task).flattenChunks
