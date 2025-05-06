package com.sneaksanddata.arcane.framework
package services.synapse

import models.ArcaneSchema
import models.batches.{MergeableBatch, StagedVersionedBatch, SynapseLinkMergeBatch}
import models.settings.TablePropertiesSettings
import services.hooks.manager.EmptyHookManager

import org.apache.iceberg.Table

/**
 * A hook manager that does nothing.
 */
class SynapseHookManager extends EmptyHookManager:
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
    SynapseLinkMergeBatch(batchName, batchSchema, targetName, tablePropertiesSettings)


object SynapseHookManager:
  /**
   * The required environment for the EmptyHookManager.
   */
  type Environment = Any

  /**
   * Creates a new empty hook manager.
   *
   * @return A new empty hook manager.
   */
  def apply(): SynapseHookManager = new SynapseHookManager()

  val layer: zio.ZLayer[Any, Nothing, SynapseHookManager] = zio.ZLayer.succeed(SynapseHookManager())
  
