package com.sneaksanddata.arcane.framework
package logging

import zio.ZIO
import zio.logging.LogAnnotation
import upickle.default.read

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
  final def getStringAnnotation(name: String): LogAnnotation[String] = LogAnnotation[String](
    name = name,
    combine = (_, s) => s,
    render = s => s
  )

  private val defaults: Seq[(LogAnnotation[String], String)] = Seq(
    (getStringAnnotation(name = "streamKind"), streamClass),
    (getStringAnnotation(name = "streamId"), streamId),
    (getStringAnnotation(name = "streamVersion"), streamVersion)
  ) ++ read[Map[String, String]](streamExtraProperties).map { (key, value) => (getStringAnnotation(key), value) }


  /**
   * @param message Log message to record
   * @param annotations Optional ZIO log annotations and values.
   * @return ZIO Workflow
   */
  @unused
  def zlog(message: String, annotations: Option[Seq[(LogAnnotation[String], String)]] = None): zio.UIO[Unit] = annotations match
    case Some(values) => logEnriched(ZIO.log(message), defaults ++ values)
    case None => logEnriched(ZIO.log(message), defaults)
