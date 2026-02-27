package com.sneaksanddata.arcane.framework
package models.settings.sources

/** Provides buffering strategies for the streaming process.
  */
enum BufferingStrategy:
  /** Buffers the data in memory in unbounded queue.
    */
  case Unbounded

  /** Buffers the data in memory in bounded queue with a limited size. If the queue is full, the stream will block the
    * upstream until space is available.
    */
  case Buffering(maxBufferSize: Int)

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
