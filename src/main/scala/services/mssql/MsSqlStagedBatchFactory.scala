package com.sneaksanddata.arcane.framework
package services.mssql

import models.batches.{SqlServerChangeTrackingMergeBatch, SqlServerChangeTrackingWatermarkBatch}
import models.schemas.ArcaneSchema
import models.settings.EmptyTablePropertiesSettings
import services.streaming.batching.StagedBatchFactory

import zio.{Task, ZIO}

class MsSqlStagedBatchFactory extends StagedBatchFactory:
  override type OutputBatch    = SqlServerChangeTrackingMergeBatch
  override type WatermarkBatch = SqlServerChangeTrackingWatermarkBatch

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
  ): Task[SqlServerChangeTrackingWatermarkBatch] =
    ZIO.succeed(SqlServerChangeTrackingWatermarkBatch(targetTableName, watermark))
