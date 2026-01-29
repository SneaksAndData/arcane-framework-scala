package com.sneaksanddata.arcane.framework
package services.mssql

import models.batches.{MergeableBatch, SqlServerChangeTrackingMergeBatch, StagedVersionedBatch}
import models.schemas.ArcaneSchema
import models.settings.TablePropertiesSettings
import services.hooks.manager.DefaultHookManager

import org.apache.iceberg.Table

/** A hook manager that does nothing.
  */
class MsSqlHookManager extends DefaultHookManager:
  /** Converts the batch to a format that can be consumed by the next processor.
    */
  def onBatchStaged(
      table: Option[Table],
      namespace: String,
      warehouse: String,
      batchSchema: ArcaneSchema,
      targetName: String,
      tablePropertiesSettings: TablePropertiesSettings,
      watermarkValue: Option[String]
  ): StagedVersionedBatch & MergeableBatch = table match
    case Some(staged) =>
      val batchName = staged.name().split('.').last
      SqlServerChangeTrackingMergeBatch(batchName, batchSchema, targetName, tablePropertiesSettings, watermarkValue)
    case None => SqlServerChangeTrackingMergeBatch.empty(watermarkValue)
    

object MsSqlHookManager:
  /** The required environment for the DefaultHookManager.
    */
  type Environment = Any

  /** Creates a new empty hook manager.
    *
    * @return
    *   A new empty hook manager.
    */
  def apply(): MsSqlHookManager = new MsSqlHookManager()

  val layer: zio.ZLayer[Any, Nothing, MsSqlHookManager] = zio.ZLayer.succeed(MsSqlHookManager())
