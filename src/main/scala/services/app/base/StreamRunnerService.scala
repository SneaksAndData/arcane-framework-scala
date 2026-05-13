package com.sneaksanddata.arcane.framework
package services.app.base

import zio.ZIO

/** A trait that represents a service that can be used to run a stream.
  */
trait StreamRunnerService:

  /** Runs the stream.
    *
    * @return
    *   A ZIO effect that represents the stream.
    */
  def run: ZIO[Any, Throwable, Unit]
