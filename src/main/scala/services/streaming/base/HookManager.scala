package com.sneaksanddata.arcane.framework
package services.streaming.base

import services.consumers.{MergeableBatch, StagedVersionedBatch}
import services.streaming.processors.transformers.StagingProcessor

import zio.Chunk

trait HookManager:
  
  /**
   *  Enriches received staging batch with metadata and converts it to in-flight batch.
   **/
  def onStagingTablesComplete(staged: Iterable[StagedVersionedBatch & MergeableBatch], index: Long, others: Chunk[Any]): StagingProcessor#OutgoingElement
