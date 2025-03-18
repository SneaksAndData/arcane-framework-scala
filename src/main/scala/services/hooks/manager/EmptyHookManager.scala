package com.sneaksanddata.arcane.framework
package services.hooks.manager

import models.settings.{OptimizeSettings, OrphanFilesExpirationSettings, SnapshotExpirationSettings}
import services.consumers.{MergeableBatch, StagedVersionedBatch}
import services.merging.models.{JdbcOptimizationRequest, JdbcOrphanFilesExpirationRequest, JdbcSnapshotExpirationRequest}
import services.streaming.base.{HookManager, OptimizationRequestConvertable, OrphanFilesExpirationRequestConvertable, SnapshotExpirationRequestConvertable}
import services.streaming.processors.transformers.{IndexedStagedBatches, StagingProcessor}

import zio.Chunk

class EmptyIndexedStagedBatches(override val groupedBySchema: Iterable[StagedVersionedBatch & MergeableBatch], override val batchIndex: Long)
  extends IndexedStagedBatches(groupedBySchema, batchIndex)
    with SnapshotExpirationRequestConvertable
    with OrphanFilesExpirationRequestConvertable
    with OptimizationRequestConvertable:

  override def getSnapshotExpirationRequest(settings: SnapshotExpirationSettings): JdbcSnapshotExpirationRequest =
    JdbcSnapshotExpirationRequest(groupedBySchema.head.targetTableName,
      settings.batchThreshold,
      settings.retentionThreshold,
      batchIndex)

  override def getOrphanFileExpirationRequest(settings: OrphanFilesExpirationSettings): JdbcOrphanFilesExpirationRequest =
    JdbcOrphanFilesExpirationRequest(groupedBySchema.head.targetTableName,
      settings.batchThreshold,
      settings.retentionThreshold,
      batchIndex)

  override def getOptimizationRequest(settings: OptimizeSettings): JdbcOptimizationRequest =
    JdbcOptimizationRequest(groupedBySchema.head.targetTableName,
      settings.batchThreshold,
      settings.fileSizeThreshold,
      batchIndex)


/**
 * A hook manager that does nothing.
 */
abstract class EmptyHookManager extends HookManager:

  /**
   * Enriches received staging batch with metadata and converts it to in-flight batch.
   * */
  override def onStagingTablesComplete(staged: Iterable[StagedVersionedBatch & MergeableBatch], index: Long, others: Chunk[Any]): StagingProcessor#OutgoingElement =
    new EmptyIndexedStagedBatches(staged, index)
