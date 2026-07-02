package com.sneaksanddata.arcane.framework
package services.pullstream

import models.batches.{PullStreamChangeTrackingMergeBatch, PullStreamChangeTrackingWatermarkOnlyBatch}
import models.schemas.ArcaneSchema
import models.settings.EmptyTablePropertiesSettings
import services.streaming.batching.StagedBatchFactory

import zio.{Task, ULayer, ZIO, ZLayer}

class PullStreamStagedBatchFactory extends StagedBatchFactory:
  val versionFieldName = "TimestampUTC"
  override type OutputBatch    = PullStreamChangeTrackingMergeBatch
  override type WatermarkBatch = PullStreamChangeTrackingWatermarkOnlyBatch

  override def createDataBatch(
      stagedTableName: String,
      targetTableName: String,
      batchSchema: ArcaneSchema
  ): Task[PullStreamChangeTrackingMergeBatch] =
    ZIO.succeed(
      PullStreamChangeTrackingMergeBatch(
        stagedTableName,
        batchSchema,
        targetTableName,
        EmptyTablePropertiesSettings,
        versionFieldName
      )
    )

  override def createWatermarkBatch(
      targetTableName: String,
      watermark: String
  ): Task[PullStreamChangeTrackingWatermarkOnlyBatch] =
    ZIO.succeed(PullStreamChangeTrackingWatermarkOnlyBatch(targetTableName, watermark))

object PullStreamStagedBatchFactory:
  val layer: ULayer[PullStreamStagedBatchFactory] = ZLayer.succeed(new PullStreamStagedBatchFactory())
