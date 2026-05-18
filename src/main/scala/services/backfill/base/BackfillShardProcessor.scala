package com.sneaksanddata.arcane.framework
package services.backfill.base

import zio.stream.ZPipeline

/** Represents a backfill stage that processes batches in a backfill stream.
 */
trait BackfillShardProcessor:
  /** The type of the batch.
   */
  type IncomingShardType
  type OutgoingShardType

  /** Processes the incoming data.
   *
   * @return
   * ZPipeline (stream source for the stream graph).
   */
  def process: ZPipeline[Any, Throwable, IncomingShardType, OutgoingShardType]
