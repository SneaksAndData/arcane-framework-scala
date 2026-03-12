package com.sneaksanddata.arcane.framework
package models.settings.sources

import upickle.ReadWriter
import upickle.implicits.key

/** Provides buffering strategies for the streaming process.
  */
sealed trait BufferingStrategy

/** Buffers the data in memory in unbounded queue.
  */
case class Unbounded() derives ReadWriter

/** ADT composed with settings class for Unbounded
  */
case class UnboundedImpl(unbounded: Unbounded) extends BufferingStrategy

/** Buffers the data in memory in bounded queue with a limited size. If the queue is full, the stream will block the
  * upstream until space is available.
  */
case class Buffering(maxBufferSize: Int) derives ReadWriter

/** ADT composed with settings class for Buffering
  */
case class BufferingImpl(buffering: Buffering) extends BufferingStrategy

case class BufferingSettings(
    unbounded: Option[Unbounded],
    buffered: Option[Buffering]
) derives ReadWriter:
  def resolveStrategy: BufferingStrategy = unbounded
    .map(UnboundedImpl(_))
    .getOrElse(
      buffered
        .map(BufferingImpl(_))
        .getOrElse(UnboundedImpl(Unbounded()))
    )

/** Provides settings for source buffering in the streaming process. Allows a faster producer to progress independently
  * of a slower consumer by buffering up to capacity elements in a queue.
  *
  * The buffering settings are applied to the data provider to allow the data provider and the later stages of the
  * pipeline run asynchronously.
  */
trait SourceBufferingSettings:
  /** The buffering strategy to use.
    */
  val bufferingStrategy: BufferingStrategy

  /** Indicates whether buffering is enabled.
    */
  val bufferingEnabled: Boolean

case class DefaultSourceBufferingSettings(
    @key("strategy") bufferingStrategySetting: BufferingSettings,
    @key("enabled") override val bufferingEnabled: Boolean
) extends SourceBufferingSettings derives ReadWriter:
  override val bufferingStrategy: BufferingStrategy = bufferingStrategySetting.resolveStrategy
