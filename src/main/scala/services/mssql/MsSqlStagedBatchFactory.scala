package com.sneaksanddata.arcane.framework
package services.mssql

import models.batches.{SqlServerChangeTrackingMergeBatch, SqlServerChangeTrackingWatermarkOnlyBatch}
import models.schemas.ArcaneSchema
import models.settings.EmptyTablePropertiesSettings
import services.streaming.batching.StagedBatchFactory

import zio.{Task, ULayer, ZIO, ZLayer}

class MsSqlStagedBatchFactory extends StagedBatchFactory:
  override type OutputBatch    = SqlServerChangeTrackingMergeBatch
  override type WatermarkBatch = SqlServerChangeTrackingWatermarkOnlyBatch

  override def createDataBatch(
      stagedTableName: String,
      targetTableName: String,
      batchSchema: ArcaneSchema
  ): Task[SqlServerChangeTrackingMergeBatch] =
    ZIO.succeed(
      SqlServerChangeTrackingMergeBatch(stagedTableName, batchSchema, targetTableName, EmptyTablePropertiesSettings)
    )

  override def createWatermarkBatch(
      targetTableName: String,
      watermark: String
  ): Task[SqlServerChangeTrackingWatermarkOnlyBatch] =
    ZIO.succeed(SqlServerChangeTrackingWatermarkOnlyBatch(targetTableName, watermark))

object MsSqlStagedBatchFactory:
  val layer: ULayer[MsSqlStagedBatchFactory] = ZLayer.succeed(new MsSqlStagedBatchFactory())
