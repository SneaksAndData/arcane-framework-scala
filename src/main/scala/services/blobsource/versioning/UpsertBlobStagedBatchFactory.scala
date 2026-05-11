package com.sneaksanddata.arcane.framework
package services.blobsource.versioning

import models.batches.{UpsertBlobMergeBatch, UpsertBlobWatermarkBatch}
import models.schemas.ArcaneSchema
import models.settings.EmptyTablePropertiesSettings
import services.streaming.batching.StagedBatchFactory

import zio.{Task, ZIO}

class UpsertBlobStagedBatchFactory extends StagedBatchFactory:
  override type OutputBatch = UpsertBlobMergeBatch
  override type WatermarkBatch = UpsertBlobWatermarkBatch

  override def createDataBatch(stagedTableName: String, targetTableName: String, batchSchema: ArcaneSchema): Task[UpsertBlobMergeBatch] =
    ZIO.succeed(UpsertBlobMergeBatch(stagedTableName, batchSchema, targetTableName, EmptyTablePropertiesSettings))

  override def createWatermarkBatch(targetTableName: String, watermark: String): Task[UpsertBlobWatermarkBatch] =
    ZIO.succeed(UpsertBlobWatermarkBatch(targetTableName, watermark))