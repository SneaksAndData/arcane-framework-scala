package com.sneaksanddata.arcane.framework
package services.streaming.processors.utils

import models.settings.{OptimizeSettings, OrphanFilesExpirationSettings, SnapshotExpirationSettings}
import services.consumers.{ArchiveableBatch, MergeableBatch, StagedVersionedBatch}
import services.merging.models.{JdbcOptimizationRequest, JdbcOrphanFilesExpirationRequest, JdbcSnapshotExpirationRequest}
import services.streaming.base.{OptimizationRequestConvertable, OrphanFilesExpirationRequestConvertable, SnapshotExpirationRequestConvertable}
import services.streaming.processors.transformers.IndexedStagedBatches

class TestIndexedStagedBatches(override val groupedBySchema: Iterable[StagedVersionedBatch & MergeableBatch & ArchiveableBatch], override val batchIndex: Long)
  extends IndexedStagedBatches(groupedBySchema, batchIndex) with SnapshotExpirationRequestConvertable with OrphanFilesExpirationRequestConvertable with OptimizationRequestConvertable:

  def getOptimizationRequest(settings: OptimizeSettings): JdbcOptimizationRequest =
    JdbcOptimizationRequest("database", settings.batchThreshold, settings.fileSizeThreshold, batchIndex)

  def getSnapshotExpirationRequest(settings: SnapshotExpirationSettings): JdbcSnapshotExpirationRequest =
    JdbcSnapshotExpirationRequest("database", settings.batchThreshold, settings.retentionThreshold, batchIndex)

  def getOrphanFileExpirationRequest(settings: OrphanFilesExpirationSettings): JdbcOrphanFilesExpirationRequest =
    JdbcOrphanFilesExpirationRequest("database", settings.batchThreshold, settings.retentionThreshold, batchIndex)
