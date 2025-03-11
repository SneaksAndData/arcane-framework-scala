package com.sneaksanddata.arcane.framework
package services.streaming.base

import services.consumers.{MergeableBatch, StagedVersionedBatch}
import services.streaming.processors.transformers.StagingProcessor

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
