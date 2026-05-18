package com.sneaksanddata.arcane.framework
package models.backfill

import models.sharding.SourceShard
import models.serialization.OffsetDateTimeRW.*

import upickle.ReadWriter

import java.time.OffsetDateTime

trait SourceBackfill:
  val id: String
  val startedAt: OffsetDateTime
  val completedAt: Option[OffsetDateTime]

  val shards: Seq[SourceShard]
  val stagedShards: Seq[SourceShard]
  val combinedShards: Seq[SourceShard]

  def isCompleted: Boolean = completedAt.isDefined

  def progress: Double = (combinedShards.size + stagedShards.size) / shards.size.toDouble / 2

// TODO: shard serialization
case class DefaultSourceBackfill(
                                  override val id: String,
                                  override val startedAt: OffsetDateTime,
                                  override val completedAt: Option[OffsetDateTime],
                                  override val shards: Seq[SourceShard],
                                  override val stagedShards: Seq[SourceShard],
                                  override val combinedShards: Seq[SourceShard]
                                ) extends SourceBackfill derives ReadWriter

object DefaultSourceBackfill:
  def apply(value: String): DefaultSourceBackfill = upickle.read[DefaultSourceBackfill](value)
  def toJson(value: DefaultSourceBackfill): String = upickle.write(value)