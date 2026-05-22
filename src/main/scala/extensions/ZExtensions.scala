package com.sneaksanddata.arcane.framework
package extensions

import models.settings.sources.SourceBufferingSettings

import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlogStream
import models.settings.sources.{BufferingImpl, SourceBufferingSettings, UnboundedImpl}
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

    def trySetBuffering(sourceBufferingSettings: SourceBufferingSettings): ZStream[Any, Throwable, Element] =
      (sourceBufferingSettings.bufferingEnabled, sourceBufferingSettings.bufferingStrategy) match
        case (true, UnboundedImpl(_)) =>
          zlogStream("Running stream with unbound source buffer") *> stream.bufferUnbounded

        case (true, BufferingImpl(buffering)) =>
          zlogStream("Running stream with bound source buffer size %s", buffering.maxBufferSize.toString) *> stream
            .buffer(buffering.maxBufferSize)

        case (false, _) => zlogStream("Running stream with disabled source buffering") *> stream
