package com.sneaksanddata.arcane.framework
package services.blobsource

import models.batches.{MergeableBatch, StagedVersionedBatch, UpsertBlobMergeBatch}
import models.schemas.ArcaneSchema
import models.settings.TablePropertiesSettings
import services.hooks.manager.DefaultHookManager

import org.apache.iceberg.Table

class UpsertBlobHookManager extends DefaultHookManager:
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
      UpsertBlobMergeBatch(batchName, batchSchema, targetName, tablePropertiesSettings, watermarkValue)
    case None => UpsertBlobMergeBatch.empty(watermarkValue)

object UpsertBlobHookManager:
  /** The required environment for the DefaultHookManager.
    */
  type Environment = Any

  /** Creates a new empty hook manager.
    *
    * @return
    *   A new empty hook manager.
    */
  def apply(): UpsertBlobHookManager = new UpsertBlobHookManager()

  val layer: zio.ZLayer[Any, Nothing, UpsertBlobHookManager] = zio.ZLayer.succeed(UpsertBlobHookManager())
