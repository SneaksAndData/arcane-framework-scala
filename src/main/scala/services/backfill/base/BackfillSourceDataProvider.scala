package com.sneaksanddata.arcane.framework
package services.backfill.base

import models.sharding.BootstrappedShard
import services.streaming.base.SourceWatermark

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
  def requestBackfill(
      snapshotVersion: DataVersionType,
      shards: Option[Seq[String]]
  ): ZStream[Any, Throwable, BootstrappedShard]

  /** Most recent version of the dataset at a time when a backfill was initiated.
    */
  def getSnapshotVersion: Task[DataVersionType]

  /** Evaluates watermark to be used when evaluating current snapshot version at the start of a backfill process
    *
    * @return
    */
  def getBackfillStartWatermark(startTime: Option[OffsetDateTime]): Task[DataVersionType]
