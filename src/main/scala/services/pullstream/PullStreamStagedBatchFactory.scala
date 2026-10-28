package com.sneaksanddata.arcane.framework
package services.pullstream

import models.batches.{PullStreamChangeTrackingMergeBatch, PullStreamChangeTrackingWatermarkOnlyBatch}
import models.schemas.ArcaneSchema
import models.settings.EmptyTablePropertiesSettings
import services.streaming.batching.StagedBatchFactory

import zio.{Task, ZIO, ZLayer}

class PullStreamStagedBatchFactory extends StagedBatchFactory:
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
        EmptyTablePropertiesSettings
      )
    )

  override def createWatermarkBatch(
      targetTableName: String,
      watermark: String
  ): Task[PullStreamChangeTrackingWatermarkOnlyBatch] =
    ZIO.succeed(PullStreamChangeTrackingWatermarkOnlyBatch(targetTableName, watermark))

object PullStreamStagedBatchFactory:
  val layer: ZLayer[Any, Nothing, PullStreamStagedBatchFactory] =
    ZLayer.succeed(new PullStreamStagedBatchFactory())
