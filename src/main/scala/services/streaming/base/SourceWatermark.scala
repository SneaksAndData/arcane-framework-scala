package com.sneaksanddata.arcane.framework
package services.streaming.base

import upickle.ReadWriter
import upickle.default._

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

/** A token used to track source data versions when streaming changes. Watermarks are stored in the target table
  * metadata once successfully committed. This ensures a stream can continue from where it left off when restarted or
  * resuming from suspend
  * @tparam VersionType
  *   Type of the version value stored in this watermark. It is always a subclass of String, and all referencing code
  *   assumes a string. Custom type is allowed here to enable Conversion and other advanced features that will only
  *   apply to Watermark version type itself.
  */
trait SourceWatermark[VersionType <: String] extends Ordered[SourceWatermark[VersionType]]:
  /** Current source version associated with this watermark
    */
  val version: VersionType

  /** Current source update/commit time associated with this watermark
    */
  val timestamp: OffsetDateTime

/** Json-serializable watermark
  */
trait JsonWatermark:
  def toJson: String

object JsonWatermark:
  def apply(value: String)(implicit rw: ReadWriter[JsonWatermark]): JsonWatermark = upickle.read(value)

object OffsetDateTimeRW {
  private val formatter = DateTimeFormatter.ISO_INSTANT

  implicit val rw: ReadWriter[OffsetDateTime] = readwriter[String].bimap[OffsetDateTime](
    offsetDateTime => formatter.format(offsetDateTime),
    str => OffsetDateTime.parse(str, formatter)
  )
}
