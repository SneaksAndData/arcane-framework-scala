package com.sneaksanddata.arcane.framework
package services.backfill

import com.sneaksanddata.arcane.framework.models.sharding.SourceShard
import com.sneaksanddata.arcane.framework.services.streaming.base.{SourceWatermark, StructuredZStream}
import zio.Task
import zio.stream.ZStream

import java.time.OffsetDateTime

/** Provides a way to retrieve a source snapshot watermarked with a specified watermark type.
 *
 * @tparam DataVersionType
 *   The type of the data version.
 */
trait BackfillSourceDataProvider[DataVersionType <: SourceWatermark[String]]:

  /** Provides the backfill data shards to the consumer.
    *
    * @return
    *   A task that represents the backfill data.
    */
  def requestBackfill: ZStream[Any, Throwable, SourceShard]

  /** Most recent version of the dataset at a time when a backfill was initiated.
   */
  def getSnapshotVersion: Task[DataVersionType]

  /**
   * Evaluates number of shards required to backfill this source.
   */
  def getShardCount: Task[Int]
