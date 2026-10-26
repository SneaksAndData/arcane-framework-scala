package com.sneaksanddata.arcane.framework
package tests.shared

import models.schemas.{ArcaneSchema, given_CanAdd_ArcaneSchema}
import services.base.StreamingSource
import services.streaming.base.TimestampOnlyWatermark

import zio.stream.ZStream
import zio.{Task, ZIO}

object TestStreamingSource extends StreamingSource:
  override type SchemaType    = ArcaneSchema
  override type WatermarkType = TimestampOnlyWatermark
  override type ShardMetadata = String

  override def getSchema: Task[SchemaType]                = ZIO.succeed(ArcaneSchema.empty())
  override def empty: SchemaType                          = ArcaneSchema.empty()
  override def deleteShards(streamId: String): Task[Unit] = ZIO.unit
  override def getShards(
      rangeStart: WatermarkType,
      rangeEnd: WatermarkType
  ): ZStream[Any, Throwable, ShardMetadata] = ZStream.empty
