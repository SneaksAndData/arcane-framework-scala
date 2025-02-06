package com.sneaksanddata.arcane.framework
package logging

import zio.{Cause, ZIO}
import zio.logging.LogAnnotation
import upickle.default.read
import zio.stream.ZStream

import scala.annotation.unused


/**
 * Logging annotations interface for all Stream Plugins.
 * Automatically enriches logs emitted by Plugins with stream identifier, stream class, version and optional properties specified in `ARCANE__LOGGING_PROPERTIES`
 */
@unused
object ZIOLogAnnotations:
  private lazy val streamClass = sys.env.getOrElse("STREAMCONTEXT__STREAM_KIND", "undefined")
  private lazy val streamId = sys.env.getOrElse("STREAMCONTEXT__STREAM_ID", "undefined")
  private lazy val streamVersion = sys.env.getOrElse("APPLICATION_VERSION", "0.0.0")
  private lazy val streamExtraProperties = sys.env.getOrElse("ARCANE__LOGGING_PROPERTIES", "{}")

  private def logEnriched(initial: zio.UIO[Unit], extra: Seq[(LogAnnotation[String], String)]) = extra
    .map { (annotation, value) => annotation(value) }
    .foldLeft(initial)(_ @@ _)

  /**
   * @param name Name for the annotation
   * @return ZIO log annotation with the provided name
   */
  private def getStringAnnotation(name: String): LogAnnotation[String] = LogAnnotation[String](
    name = name,
    combine = (_, s) => s,
    render = s => s
  )

  /**
   * @param name  Name for the annotation
   * @param value String value to assign
   * @return
   */
  @unused
  final def getAnnotation(name: String, value: String): (LogAnnotation[String], String) = (getStringAnnotation(name), value)

  private val defaults: Seq[(LogAnnotation[String], String)] = Seq(
    (getStringAnnotation(name = "streamKind"), streamClass),
    (getStringAnnotation(name = "streamId"), streamId),
    (getStringAnnotation(name = "ApplicationVersion"), streamVersion)
  ) ++ read[Map[String, String]](streamExtraProperties).map { (key, value) => (getStringAnnotation(key), value) }


  /**
   * Log using default annotations
   *
   * @param message     Log message to record
   * @return ZIO Workflow
   */
  @unused
  def zlog(message: String): zio.UIO[Unit] = logEnriched(ZIO.log(message), defaults)

  /**
   * Log using default and additional custom annotations
   *
   * @param message     Log message to record
   * @param annotations ZIO log annotations and values
   * @return ZIO Workflow
   */
  @unused
  def zlog(message: String, annotations: Seq[(LogAnnotation[String], String)]): zio.UIO[Unit] = logEnriched(ZIO.log(message), defaults ++ annotations)

  /**
   * Log error using default annotations
   *
   * @param message     Log message to record
   * @param cause       Error cause
   * @return ZIO Workflow
   */
  @unused
  def zlog(message: String, cause: Cause[Any]): zio.UIO[Unit] = logEnriched(ZIO.logErrorCause(message, cause), defaults)

  /**
   * Log error using default and additional custom annotations
   *
   * @param message     Log message to record
   * @param cause       Error cause
   * @param annotations ZIO log annotations and values
   * @return ZIO Workflow
   */
  @unused
  def zlog(message: String, cause: Cause[Any], annotations: Seq[(LogAnnotation[String], String)]): zio.UIO[Unit] = logEnriched(ZIO.logErrorCause(message, cause), defaults ++ annotations)

  /**
   * Log via a ZStream log pipeline using default annotations
   *
   * @param message     Log message to record
   * @return ZIO Workflow
   */
  @unused
  def zlogStream(message: String): ZStream[Any, Nothing, Unit] = ZStream.fromZIO(logEnriched(ZIO.log(message), defaults))

  /**
   * Log via a ZStream log pipeline using default and additional custom annotations
   *
   * @param message     Log message to record
   * @param annotations Optional ZIO log annotations and values.
   * @return ZIO Workflow
   */
  @unused
  def zlogStream(message: String, annotations: Seq[(LogAnnotation[String], String)]): ZStream[Any, Nothing, Unit] = ZStream.fromZIO(logEnriched(ZIO.log(message), defaults ++ annotations))

  /**
   * Log error via a ZStream log pipeline using default annotations
   *
   * @param message     Log message to record
   * @param annotations Optional ZIO log annotations and values.
   * @return ZIO Workflow
   */
  @unused
  def zlogStream(message: String, cause: Cause[Any]): ZStream[Any, Nothing, Unit] = ZStream.fromZIO(logEnriched(ZIO.logErrorCause(message, cause), defaults))

  /**
   * Log error via a ZStream log pipeline using default and additional custom annotations
   *
   * @param message     Log message to record
   * @param annotations Optional ZIO log annotations and values.
   * @return ZIO Workflow
   */
  @unused
  def zlogStream(message: String, cause: Cause[Any], annotations: Seq[(LogAnnotation[String], String)]): ZStream[Any, Nothing, Unit] = ZStream.fromZIO(logEnriched(ZIO.logErrorCause(message, cause), defaults ++ annotations))
