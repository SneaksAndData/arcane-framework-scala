package com.sneaksanddata.arcane.framework
package extensions

import exceptions.{FatalStreamFailException, TransientStreamFailException}
import logging.ZIOLogAnnotations.{zlog, zlogStream}
import models.settings.sources.{BufferingImpl, SourceBufferingSettings, UnboundedImpl}

import zio.stream.ZStream
import zio.{Task, ZIO}

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

  /** Extension for plugins to handle failures gracefully.
    */
  extension (app: ZIO[Any, Throwable, Unit])
    def handleAppFailure(exitHandler: zio.ExitCode => Task[Unit]): ZIO[Any, Throwable, Unit] = app.catchAllCause {
      cause =>
        (cause.isInterrupted, cause.isDie, cause.isFailure) match
          case (true, _, _) =>
            for
              _ <- zlog("Received SIGTERM/SIGINT, stopping gracefully")
              _ <- exitHandler(zio.ExitCode(0))
            yield ()
          // all Die calls are hard failures
          case (false, true, _) =>
            for
              _ <- zlog(s"Fatal error encountered: ${cause.squashTrace.getMessage}", cause)
              _ <- exitHandler(zio.ExitCode(1))
            yield ()
          case (false, false, true) =>
            for
              exitCode <- ZIO.succeed {
                cause.failureOption match
                  case Some(failure) =>
                    failure match
                      case fse: FatalStreamFailException     => zio.ExitCode(1)
                      case tse: TransientStreamFailException => zio.ExitCode(2)
                  case None => zio.ExitCode(1)
              }
              _ <- ZIO.ifZIO(ZIO.succeed(exitCode.code == 1))(
                zlog(s"Stream instructed to fail, reason: ${cause.squashTrace.getMessage}", cause),
                zlog(s"Stream instructed to fail, reason: ${cause.squashTrace.getMessage}", cause)
              )
              _ <- exitHandler(exitCode)
            yield ()
          // treat unknown Failure as a transient restart
          case _ =>
            for
              _ <- zlog(s"Stream instructed to restart, reason: ${cause.squashTrace.getMessage}", cause)
              _ <- exitHandler(zio.ExitCode(2))
            yield ()
    }
