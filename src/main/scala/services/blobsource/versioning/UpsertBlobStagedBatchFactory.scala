package com.sneaksanddata.arcane.framework
package services.blobsource.versioning

import models.batches.{UpsertBlobMergeBatch, UpsertBlobWatermarkOnlyBatch}
import models.schemas.ArcaneSchema
import models.settings.EmptyTablePropertiesSettings
import services.streaming.batching.StagedBatchFactory

import zio.{Task, ULayer, ZIO, ZLayer}

class UpsertBlobStagedBatchFactory extends StagedBatchFactory:
  override type OutputBatch    = UpsertBlobMergeBatch
  override type WatermarkBatch = UpsertBlobWatermarkOnlyBatch

  override def createDataBatch(
      stagedTableName: String,
      targetTableName: String,
      batchSchema: ArcaneSchema
  ): Task[UpsertBlobMergeBatch] =
    ZIO.succeed(UpsertBlobMergeBatch(stagedTableName, batchSchema, targetTableName, EmptyTablePropertiesSettings))

  override def createWatermarkBatch(targetTableName: String, watermark: String): Task[UpsertBlobWatermarkOnlyBatch] =
    ZIO.succeed(UpsertBlobWatermarkOnlyBatch(targetTableName, watermark))

object UpsertBlobStagedBatchFactory:
  val layer: ULayer[UpsertBlobStagedBatchFactory] = ZLayer.succeed(new UpsertBlobStagedBatchFactory())
