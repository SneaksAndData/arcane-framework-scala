package com.sneaksanddata.arcane.framework
package services.hooks.manager

import models.batches.{MergeableBatch, StagedVersionedBatch}
import models.settings.{AnalyzeSettings, OptimizeSettings, OrphanFilesExpirationSettings, SnapshotExpirationSettings}
import services.merging.maintenance.{
  JdbcAnalyzeRequest,
  JdbcOptimizationRequest,
  JdbcOrphanFilesExpirationRequest,
  JdbcSnapshotExpirationRequest
}
import services.streaming.base.{
  AnalyzeRequestConvertable,
  HookManager,
  OptimizationRequestConvertable,
  OrphanFilesExpirationRequestConvertable,
  SnapshotExpirationRequestConvertable
}
import services.streaming.processors.transformers.{IndexedStagedBatches, StagingProcessor}

import zio.Chunk

class DefaultIndexedStagedBatches(groupedBySchema: Iterable[StagedVersionedBatch & MergeableBatch], batchIndex: Long)
    extends IndexedStagedBatches(groupedBySchema, batchIndex)
    with SnapshotExpirationRequestConvertable
    with OrphanFilesExpirationRequestConvertable
    with OptimizationRequestConvertable
    with AnalyzeRequestConvertable:

  override def getSnapshotExpirationRequest(
      settings: Option[SnapshotExpirationSettings]
  ): Option[JdbcSnapshotExpirationRequest] = settings.map { snapshotExpirationSettings =>
    JdbcSnapshotExpirationRequest(
      groupedBySchema.head.targetTableName,
      snapshotExpirationSettings.batchThreshold,
      snapshotExpirationSettings.retentionThreshold,
      batchIndex
    )
  }

  override def getOrphanFileExpirationRequest(
      settings: Option[OrphanFilesExpirationSettings]
  ): Option[JdbcOrphanFilesExpirationRequest] = settings.map { orphanFilesExpirationSettings =>
    JdbcOrphanFilesExpirationRequest(
      groupedBySchema.head.targetTableName,
      orphanFilesExpirationSettings.batchThreshold,
      orphanFilesExpirationSettings.retentionThreshold,
      batchIndex
    )
  }

  override def getOptimizationRequest(settings: Option[OptimizeSettings]): Option[JdbcOptimizationRequest] =
    settings.map { optimizerSettings =>
      JdbcOptimizationRequest(
        groupedBySchema.head.targetTableName,
        optimizerSettings.batchThreshold,
        optimizerSettings.fileSizeThreshold,
        batchIndex
      )
    }

  override def getAnalyzeRequest(settings: Option[AnalyzeSettings]): Option[JdbcAnalyzeRequest] =
    settings.map { analyzerSettings =>
      JdbcAnalyzeRequest(
        groupedBySchema.head.targetTableName,
        analyzerSettings.batchThreshold,
        analyzerSettings.includedColumns,
        batchIndex
      )
    }

/** A hook manager that performs standard operations based on batchIndex:
 * OPTIMIZE
 * Expire Snapshots
 * Remove Orphans
 * ANALYZE.
  */
abstract class DefaultHookManager extends HookManager:

  /** Enriches received staging batch with metadata and converts it to in-flight batch.
    */
  override def onStagingTablesComplete(
      staged: Iterable[StagedVersionedBatch & MergeableBatch],
      index: Long,
      others: Chunk[Any]
  ): StagingProcessor#OutgoingElement =
    new DefaultIndexedStagedBatches(staged, index)
