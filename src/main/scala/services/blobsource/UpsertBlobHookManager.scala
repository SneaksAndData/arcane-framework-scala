package com.sneaksanddata.arcane.framework
package services.blobsource

import models.batches.{MergeableBatch, StagedVersionedBatch, UpsertBlobMergeBatch}
import models.schemas.ArcaneSchema
import models.settings.TablePropertiesSettings
import services.hooks.manager.EmptyHookManager

import org.apache.iceberg.Table

class UpsertBlobHookManager extends EmptyHookManager:
  /** Converts the batch to a format that can be consumed by the next processor.
    */
  def onBatchStaged(
      table: Table,
      namespace: String,
      warehouse: String,
      batchSchema: ArcaneSchema,
      targetName: String,
      tablePropertiesSettings: TablePropertiesSettings
  ): StagedVersionedBatch & MergeableBatch =
    val batchName = table.name().split('.').last
    UpsertBlobMergeBatch(batchName, batchSchema, targetName, tablePropertiesSettings)

object UpsertBlobHookManager:
  /** The required environment for the EmptyHookManager.
    */
  type Environment = Any

  /** Creates a new empty hook manager.
    *
    * @return
    *   A new empty hook manager.
    */
  def apply(): UpsertBlobHookManager = new UpsertBlobHookManager()

  val layer: zio.ZLayer[Any, Nothing, UpsertBlobHookManager] = zio.ZLayer.succeed(UpsertBlobHookManager())
