package com.sneaksanddata.arcane.framework
package services.pushstream

import models.batches.{PushStreamChangeTrackingMergeBatch, PushStreamChangeTrackingWatermarkOnlyBatch}
import models.schemas.ArcaneSchema
import models.settings.EmptyTablePropertiesSettings
import services.streaming.batching.StagedBatchFactory

import zio.{Task, ULayer, ZIO, ZLayer}

class PushStreamStagedBatchFactory extends StagedBatchFactory:
  val versionFieldName = "TimestampUTC"
  override type OutputBatch    = PushStreamChangeTrackingMergeBatch
  override type WatermarkBatch = PushStreamChangeTrackingWatermarkOnlyBatch

  override def createDataBatch(
      stagedTableName: String,
      targetTableName: String,
      batchSchema: ArcaneSchema
  ): Task[PushStreamChangeTrackingMergeBatch] =
    ZIO.succeed(
      PushStreamChangeTrackingMergeBatch(
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
  ): Task[PushStreamChangeTrackingWatermarkOnlyBatch] =
    ZIO.succeed(PushStreamChangeTrackingWatermarkOnlyBatch(targetTableName, watermark))

object PushStreamStagedBatchFactory:
  val layer: ULayer[PushStreamStagedBatchFactory] = ZLayer.succeed(new PushStreamStagedBatchFactory())
