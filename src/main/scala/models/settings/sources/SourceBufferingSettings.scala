package com.sneaksanddata.arcane.framework
package models.settings.sources

import upickle.ReadWriter
import upickle.implicits.key

/** Provides buffering strategies for the streaming process.
  */
sealed trait BufferingStrategy

/** Buffers the data in memory in unbounded queue.
  */
case class Unbounded() extends BufferingStrategy derives ReadWriter

/** Buffers the data in memory in bounded queue with a limited size. If the queue is full, the stream will block the
  * upstream until space is available.
  */
case class Buffering(maxBufferSize: Int) extends BufferingStrategy derives ReadWriter

case class BufferingSettings(
    unbounded: Option[Unbounded] = None,
    buffered: Option[Buffering] = None
) derives ReadWriter:
  def resolveStrategy: BufferingStrategy = unbounded.getOrElse(buffered.getOrElse(Unbounded()))

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
