package com.sneaksanddata.arcane.framework
package logging

import zio.{Cause, ZIO}
import zio.logging.LogAnnotation
import upickle.default.read
import zio.stream.ZStream

import scala.annotation.unused
import extensions.StringExtensions.camelCaseToSnakeCase


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
    (getStringAnnotation(name = "streamKind"), streamClass.camelCaseToSnakeCase),
    (getStringAnnotation(name = "streamId"), streamId.camelCaseToSnakeCase),
    (getStringAnnotation(name = "ApplicationVersion"), streamVersion.camelCaseToSnakeCase)
  ) ++ read[Map[String, String]](streamExtraProperties).map { (key, value) => (getStringAnnotation(key), value) }

  private def defaultsWithTemplate(template: String): Seq[(LogAnnotation[String], String)] =
    defaults ++ Seq((getStringAnnotation(name = "messageTemplate"), template))

  
  /**
   * Log using default annotations
   *
   * @param template  Log message template to use
   * @param values Values for rendering the template
   * @return ZIO Workflow
   */
  @unused
  def zlog(template: String, values: String*): zio.UIO[Unit] = logEnriched(ZIO.log(template.format(values*)), defaultsWithTemplate(template))

  /**
   * Log using default and additional custom annotations
   *
   * @param template     Log message to record
   * @param annotations ZIO log annotations and values
   * @param values Values for rendering the template
   * @return ZIO Workflow
   */
  @unused
  def zlog(template: String, annotations: Seq[(LogAnnotation[String], String)], values: String*): zio.UIO[Unit] = logEnriched(ZIO.log(template.format(values*)), defaultsWithTemplate(template) ++ annotations)

  /**
   * Log error using default annotations
   *
   * @param template     Log message to record
   * @param cause       Error cause
   * @param values Values for rendering the template
   * @return ZIO Workflow
   */
  @unused
  def zlog(template: String, cause: Cause[Any], values: String*): zio.UIO[Unit] = logEnriched(ZIO.logErrorCause(template.format(values*), cause), defaultsWithTemplate(template))

  /**
   * Log error using default and additional custom annotations
   *
   * @param template     Log message to record
   * @param cause       Error cause
   * @param annotations ZIO log annotations and values
   * @param values Values for rendering the template
   * @return ZIO Workflow
   */
  @unused
  def zlog(template: String, cause: Cause[Any], annotations: Seq[(LogAnnotation[String], String)], values: String*): zio.UIO[Unit] = logEnriched(ZIO.logErrorCause(template.format(values*), cause), defaultsWithTemplate(template) ++ annotations)

  /**
   * Log via a ZStream log pipeline using default annotations
   *
   * @param template     Log message to record
   * @param values Values for rendering the template
   * @return ZIO Workflow
   */
  @unused
  def zlogStream(template: String, values: String*): ZStream[Any, Nothing, Unit] = ZStream.fromZIO(logEnriched(ZIO.log(template.format(values*)), defaultsWithTemplate(template)))

  /**
   * Log via a ZStream log pipeline using default and additional custom annotations
   *
   * @param template     Log message to record
   * @param annotations Optional ZIO log annotations and values.
   * @param values Values for rendering the template
   * @return ZIO Workflow
   */
  @unused
  def zlogStream(template: String, annotations: Seq[(LogAnnotation[String], String)], values: String*): ZStream[Any, Nothing, Unit] = ZStream.fromZIO(logEnriched(ZIO.log(template.format(values*)), defaultsWithTemplate(template) ++ annotations))

  /**
   * Log error via a ZStream log pipeline using default annotations
   *
   * @param template     Log message to record
   * @param cause Error cause.
   * @param values Values for rendering the template
   * @return ZIO Workflow
   */
  @unused
  def zlogStream(template: String, cause: Cause[Any], values: String*): ZStream[Any, Nothing, Unit] = ZStream.fromZIO(logEnriched(ZIO.logErrorCause(template.format(values*), cause), defaultsWithTemplate(template)))

  /**
   * Log error via a ZStream log pipeline using default and additional custom annotations
   *
   * @param template     Log message to record
   * @param annotations Optional ZIO log annotations and values.
   * @param values Values for rendering the template
   * @return ZIO Workflow
   */
  @unused
  def zlogStream(template: String, cause: Cause[Any], annotations: Seq[(LogAnnotation[String], String)], values: String*): ZStream[Any, Nothing, Unit] = ZStream.fromZIO(logEnriched(ZIO.logErrorCause(template.format(values*), cause), defaultsWithTemplate(template) ++ annotations))
