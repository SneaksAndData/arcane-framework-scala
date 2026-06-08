package com.sneaksanddata.arcane.framework
package services.synapse

import models.batches.{SynapseLinkMergeBatch, SynapseLinkWatermarkOnlyBatch}
import models.schemas.ArcaneSchema
import models.settings.EmptyTablePropertiesSettings
import services.streaming.batching.StagedBatchFactory

import zio.{Task, ULayer, ZIO, ZLayer}

class SynapseBatchFactory extends StagedBatchFactory:
  override type OutputBatch    = SynapseLinkMergeBatch
  override type WatermarkBatch = SynapseLinkWatermarkOnlyBatch

  override def createDataBatch(
      stagedTableName: String,
      targetTableName: String,
      batchSchema: ArcaneSchema
  ): Task[SynapseLinkMergeBatch] =
    ZIO.succeed(SynapseLinkMergeBatch(stagedTableName, batchSchema, targetTableName, EmptyTablePropertiesSettings))

  override def createWatermarkBatch(targetTableName: String, watermark: String): Task[SynapseLinkWatermarkOnlyBatch] =
    ZIO.succeed(SynapseLinkWatermarkOnlyBatch(watermark, targetTableName))

object SynapseBatchFactory:
  val layer: ULayer[SynapseBatchFactory] = ZLayer.succeed(new SynapseBatchFactory())
