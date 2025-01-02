package com.sneaksanddata.arcane.framework
package services.app.base

import services.streaming.base.StreamGraphBuilder

import zio.ZIO
import zio.stream.ZSink

/**
  * A trait that represents a service that can be used to run a stream.
 */
trait StreamRunnerService:
  
  /**
    * Runs the stream.
    *
    * @return A ZIO effect that represents the stream.
    */
  def run: ZIO[Any, Throwable, Unit]
