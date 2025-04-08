package com.sneaksanddata.arcane.framework
package services.hooks.manager

import models.settings.{OptimizeSettings, OrphanFilesExpirationSettings, SnapshotExpirationSettings}
import services.consumers.{MergeableBatch, StagedVersionedBatch}
import services.merging.models.{JdbcOptimizationRequest, JdbcOrphanFilesExpirationRequest, JdbcSnapshotExpirationRequest}
import services.streaming.base.{HookManager, OptimizationRequestConvertable, OrphanFilesExpirationRequestConvertable, SnapshotExpirationRequestConvertable}
import services.streaming.processors.transformers.{IndexedStagedBatches, StagingProcessor}

import zio.Chunk

class EmptyIndexedStagedBatches(groupedBySchema: Iterable[StagedVersionedBatch & MergeableBatch], batchIndex: Long)
  extends IndexedStagedBatches(groupedBySchema, batchIndex)
    with SnapshotExpirationRequestConvertable
    with OrphanFilesExpirationRequestConvertable
    with OptimizationRequestConvertable:

  override def getSnapshotExpirationRequest(settings: Option[SnapshotExpirationSettings]): Option[JdbcSnapshotExpirationRequest] = settings.map { snapshotExpirationSettings =>
    JdbcSnapshotExpirationRequest(groupedBySchema.head.targetTableName,
      snapshotExpirationSettings.batchThreshold,
      snapshotExpirationSettings.retentionThreshold,
      batchIndex)
  }

  override def getOrphanFileExpirationRequest(settings: Option[OrphanFilesExpirationSettings]): Option[JdbcOrphanFilesExpirationRequest] = settings.map { orphanFilesExpirationSettings =>
    JdbcOrphanFilesExpirationRequest(groupedBySchema.head.targetTableName,
      orphanFilesExpirationSettings.batchThreshold,
      orphanFilesExpirationSettings.retentionThreshold,
      batchIndex) 
  }

  override def getOptimizationRequest(settings: Option[OptimizeSettings]): Option[JdbcOptimizationRequest] = settings.map { optimizerSettings =>
    JdbcOptimizationRequest(groupedBySchema.head.targetTableName,
      optimizerSettings.batchThreshold,
      optimizerSettings.fileSizeThreshold,
      batchIndex)
  }
      


/**
 * A hook manager that does nothing.
 */
abstract class EmptyHookManager extends HookManager:

  /**
   * Enriches received staging batch with metadata and converts it to in-flight batch.
   * */
  override def onStagingTablesComplete(staged: Iterable[StagedVersionedBatch & MergeableBatch], index: Long, others: Chunk[Any]): StagingProcessor#OutgoingElement =
    new EmptyIndexedStagedBatches(staged, index)
