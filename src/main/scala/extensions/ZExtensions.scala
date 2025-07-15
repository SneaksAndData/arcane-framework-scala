package com.sneaksanddata.arcane.framework
package extensions

import zio.Task
import zio.stream.ZStream

/** Extensions for ZIO objects
  */
object ZExtensions:

  /** Executes an effect once a stream completes. Effect execution is transparent to any downstream element receiver or
    * a ZSink, but it will block downstream processing until the effect has finished running. This method does not
    * complete the affected ZStream.
    */
  extension [Element](stream: ZStream[Any, Throwable, Element])
    def onComplete(effect: Task[Unit]): ZStream[Any, Throwable, Element] =
      stream.concat(ZStream.fromZIO(effect).flatMap(_ => ZStream.empty))
