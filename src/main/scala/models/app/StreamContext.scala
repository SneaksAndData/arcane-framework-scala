package com.sneaksanddata.arcane.framework
package models.app

/**
 * Provides the context for the stream.
 */
trait StreamContext:

  /**
   * The id of the stream.
   */
  def streamId: String = sys.env("STREAMCONTEXT__STREAM_ID")

  /**
   * True if the stream is running in backfill mode.
   */
  def IsBackfilling: Boolean = sys.env.getOrElse("STREAMCONTEXT__BACKFILL", "false").toLowerCase() == "true"

  /**
   * Kind of the stream
   */
  def streamKind: String = sys.env("STREAMCONTEXT__STREAM_KIND")

