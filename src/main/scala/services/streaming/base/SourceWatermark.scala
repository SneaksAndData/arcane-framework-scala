package com.sneaksanddata.arcane.framework
package services.streaming.base

import upickle.ReadWriter
import upickle.default.*

import java.time.{Duration, OffsetDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

/**
 * Base trait that represents and objects holding source version tracking information.
 * In its most simple form, contains a single timestamp from source. This timestamp corresponds to the time data was last updated in the source.
 */
trait Watermark:
  
  /** Current source update/commit time associated with this watermark
   */
  val timestamp: OffsetDateTime

  /**
   * Age of this watermark at the current moment of time
   *
   * @return
   */
  def age: Long = Duration.between(timestamp, OffsetDateTime.now()).toSeconds
  
/** A token used to track source data versions when streaming changes. Watermarks are stored in the target table
  * metadata once successfully committed. This ensures a stream can continue from where it left off when restarted or
  * resuming from suspend
  * @tparam VersionType
  *   Type of the version value stored in this watermark. It is always a subclass of String, and all referencing code
  *   assumes a string. Custom type is allowed here to enable Conversion and other advanced features that will only
  *   apply to Watermark version type itself.
  */
trait SourceWatermark[VersionType <: String] extends Ordered[SourceWatermark[VersionType]] with Watermark:
  /** Current source version associated with this watermark
    */
  val version: VersionType

/** Json-serializable watermark
  */
trait JsonWatermark:
  def toJson: String

object JsonWatermark:
  def apply(value: String)(implicit rw: ReadWriter[JsonWatermark]): JsonWatermark = upickle.read(value)


case class TimestampOnlyWatermark(timestamp: OffsetDateTime) extends Watermark with JsonWatermark:
  override def toJson: String = upickle.write(this)

object  TimestampOnlyWatermark:
  import OffsetDateTimeRW._
  
  implicit val rw: ReadWriter[TimestampOnlyWatermark] = macroRW

  def fromJson(value: String): TimestampOnlyWatermark = upickle.read(value)


object OffsetDateTimeRW:
  private val formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME

  implicit val rw: ReadWriter[OffsetDateTime] = readwriter[String].bimap[OffsetDateTime](
    offsetDateTime => formatter.format(offsetDateTime),
    str => OffsetDateTime.parse(str, formatter)
  )

