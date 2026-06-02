package com.sneaksanddata.arcane.framework
package services.base

import zio.Task
import zio.stream.ZStream

trait ShardProvider:
  type ShardMetadata
  type WatermarkType

  /** Deletes all shards created for the provided streamId
    */
  def deleteShards(prefix: String): Task[Unit]

  /** Retrieve a shard data stream
    * @return
    */
  def getShards(backfillId: String, rangeStart: WatermarkType, rangeEnd: WatermarkType): ZStream[Any, Throwable, ShardMetadata]
