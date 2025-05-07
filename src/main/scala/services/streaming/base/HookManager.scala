package com.sneaksanddata.arcane.framework
package services.streaming.base

import models.batches.{MergeableBatch, StagedVersionedBatch}
import models.schemas.ArcaneSchema
import models.settings.TablePropertiesSettings
import services.streaming.processors.transformers.StagingProcessor

import org.apache.iceberg.Table
import zio.Chunk

/**
 * Manages hooks for the streaming process.
 * Hooks can be used to enrich the data, log the results, etc. in future implementations.
 */
trait HookManager:
  
  /**
   *  Enriches received staging batch with metadata and converts it to in-flight batch.
   **/
  def onStagingTablesComplete(staged: Iterable[StagedVersionedBatch & MergeableBatch], index: Long, others: Chunk[Any]): StagingProcessor#OutgoingElement

  /**
   * Converts the batch to a format that can be consumed by the next processor.
   * */
  def onBatchStaged(table: Table, 
                    namespace: String,
                    warehouse: String,
                    batchSchema: ArcaneSchema,
                    targetName: String,
                    tablePropertiesSettings: TablePropertiesSettings): StagedVersionedBatch & MergeableBatch
