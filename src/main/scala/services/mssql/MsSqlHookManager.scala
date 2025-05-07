package com.sneaksanddata.arcane.framework
package services.mssql

import models.batches.{MergeableBatch, SqlServerChangeTrackingMergeBatch, StagedVersionedBatch}
import models.schemas.ArcaneSchema
import models.settings.TablePropertiesSettings
import services.hooks.manager.EmptyHookManager

import org.apache.iceberg.Table

/**
 * A hook manager that does nothing.
 */
class MsSqlHookManager extends EmptyHookManager:
  /**
   * Converts the batch to a format that can be consumed by the next processor.
   * */
  def onBatchStaged(table: Table,
                    namespace: String,
                    warehouse: String,
                    batchSchema: ArcaneSchema,
                    targetName: String,
                    tablePropertiesSettings: TablePropertiesSettings): StagedVersionedBatch & MergeableBatch =
    val batchName = table.name().split('.').last
    SqlServerChangeTrackingMergeBatch(batchName, batchSchema, targetName, tablePropertiesSettings)


object MsSqlHookManager:
  /**
   * The required environment for the EmptyHookManager.
   */
  type Environment = Any

  /**
   * Creates a new empty hook manager.
   *
   * @return A new empty hook manager.
   */
  def apply(): MsSqlHookManager = new MsSqlHookManager()

  val layer: zio.ZLayer[Any, Nothing, MsSqlHookManager] = zio.ZLayer.succeed(MsSqlHookManager())
  
