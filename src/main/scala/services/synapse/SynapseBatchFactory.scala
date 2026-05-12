package com.sneaksanddata.arcane.framework
package services.synapse

import models.batches.{SynapseLinkMergeBatch, SynapseLinkWatermarkBatch}
import models.schemas.ArcaneSchema
import models.settings.EmptyTablePropertiesSettings
import services.streaming.batching.StagedBatchFactory

import zio.{Task, ZIO}

class SynapseBatchFactory extends StagedBatchFactory:
  override type OutputBatch    = SynapseLinkMergeBatch
  override type WatermarkBatch = SynapseLinkWatermarkBatch

  override def createDataBatch(
      stagedTableName: String,
      targetTableName: String,
      batchSchema: ArcaneSchema
  ): Task[SynapseLinkMergeBatch] =
    ZIO.succeed(SynapseLinkMergeBatch(stagedTableName, batchSchema, targetTableName, EmptyTablePropertiesSettings))

  override def createWatermarkBatch(targetTableName: String, watermark: String): Task[SynapseLinkWatermarkBatch] =
    ZIO.succeed(SynapseLinkWatermarkBatch(watermark, targetTableName))
