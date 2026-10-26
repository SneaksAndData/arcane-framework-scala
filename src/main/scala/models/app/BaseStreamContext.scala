package com.sneaksanddata.arcane.framework
package models.app

import exceptions.FatalStreamFailException
import models.settings.{BackfillIdentifier, StreamIdentifier}

import upickle.ReadWriter
import zio.{IO, ZIO}

import java.time.Duration

enum ApplicationRunMode derives ReadWriter:
  case Stream, Backfill, Migrate

/** Provides the context for the stream.
  */
trait BaseStreamContext:

  /** The id of the stream.
    */
  def streamId: IO[SecurityException, StreamIdentifier] = zio.System.env("STREAMCONTEXT__STREAM_ID").map {
    case Some(value) => value
    case None =>
      throw FatalStreamFailException(
        "Unable to bootstrap the stream, missing required STREAMCONTEXT__STREAM_ID environment variable"
      )
  }

  /** True if the stream is running in backfill mode.
    */
  def isBackfilling: ZIO[Any, SecurityException, Boolean] =
    zio.System.envOrElse("STREAMCONTEXT__BACKFILL", "false").map(_.toLowerCase() == "true")

  def isMigrating: ZIO[Any, SecurityException, Boolean] =
    zio.System.envOrElse("STREAMCONTEXT__RUNMODE", ApplicationRunMode.Stream.toString.toLowerCase).map {
      case value if value == ApplicationRunMode.Stream.toString.toLowerCase   => false
      case value if value == ApplicationRunMode.Backfill.toString.toLowerCase => false
      case value if value == ApplicationRunMode.Migrate.toString.toLowerCase  => true
      case _ =>
        throw FatalStreamFailException(
          s"Unsupported or missing application run mode. STREAMCONTEXT__RUNMODE must be set to one of: ${ApplicationRunMode.values.mkString("/")} "
        )
    }

  /** Identifier for the backfill. Providing the same value in the env variable will result in resuming the backfill if
    * it was interrupted
    */
  def backfillId: ZIO[Any, SecurityException, BackfillIdentifier] = zio.System
    .envOrElse("STREAMCONTEXT__BACKFILL_ID", "")

  /** Kind of the stream
    */
  def streamKind: IO[SecurityException, String] = zio.System.env("STREAMCONTEXT__STREAM_KIND").map {
    case Some(value) => value
    case None =>
      throw FatalStreamFailException(
        "Unable to bootstrap the stream, missing required STREAMCONTEXT__STREAM_KIND environment variable"
      )
  }

  /** Version of the streaming plugin
    * @return
    */
  def streamVersion: IO[SecurityException, String] = zio.System.envOrElse("APPLICATION_VERSION", "0.0.0")

  val datadogSocketPath: String =
    sys.env.getOrElse("ARCANE_FRAMEWORK__DATADOG_SOCKET_PATH", "/var/run/datadog/dsd.socket")

  val metricsPublisherInterval: Duration = sys.env.get("ARCANE_FRAMEWORK__METRICS_PUBLISHER_INTERVAL_MILLIS") match
    case Some(value) => Duration.ofMillis(value.toInt)
    case None        => Duration.ofSeconds(10)
