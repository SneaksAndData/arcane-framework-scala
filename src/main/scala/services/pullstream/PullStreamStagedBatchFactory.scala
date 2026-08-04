package com.sneaksanddata.arcane.framework
package services.pullstream

import models.batches.{PullStreamChangeTrackingMergeBatch, PullStreamChangeTrackingWatermarkOnlyBatch}
import models.schemas.ArcaneSchema
import models.settings.EmptyTablePropertiesSettings
import services.streaming.batching.StagedBatchFactory

import zio.{Task, ZIO, ZLayer}

/** @param versionFieldName
  *   Column used to order concurrent versions of the same merge key. It must be a column of the sink table, and is
  *   taken from the source's `watermarkFieldName` so that it always matches the value the source projects into rows.
  */
class PullStreamStagedBatchFactory(val versionFieldName: String) extends StagedBatchFactory:
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
  val layer: ZLayer[PullStreamingSource, Nothing, PullStreamStagedBatchFactory] =
    ZLayer {
      for source <- ZIO.service[PullStreamingSource]
      yield new PullStreamStagedBatchFactory(source.versionFieldName)
    }
