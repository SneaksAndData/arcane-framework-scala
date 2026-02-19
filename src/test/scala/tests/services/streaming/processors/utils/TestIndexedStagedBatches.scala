package com.sneaksanddata.arcane.framework
package tests.services.streaming.processors.utils

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
  OptimizationRequestConvertable,
  OrphanFilesExpirationRequestConvertable,
  SnapshotExpirationRequestConvertable
}
import services.streaming.processors.transformers.IndexedStagedBatches

class TestIndexedStagedBatches(
    override val groupedBySchema: StagedVersionedBatch & MergeableBatch,
    override val batchIndex: Long
) extends IndexedStagedBatches(groupedBySchema, batchIndex)
    with SnapshotExpirationRequestConvertable
    with OrphanFilesExpirationRequestConvertable
    with OptimizationRequestConvertable
    with AnalyzeRequestConvertable:

  def getOptimizationRequest(settings: Option[OptimizeSettings]): Option[JdbcOptimizationRequest] = settings.map { s =>
    JdbcOptimizationRequest("database", s.batchThreshold, s.fileSizeThreshold, batchIndex)
  }

  def getSnapshotExpirationRequest(
      settings: Option[SnapshotExpirationSettings]
  ): Option[JdbcSnapshotExpirationRequest] = settings.map { s =>
    JdbcSnapshotExpirationRequest("database", s.batchThreshold, s.retentionThreshold, batchIndex)
  }

  def getOrphanFileExpirationRequest(
      settings: Option[OrphanFilesExpirationSettings]
  ): Option[JdbcOrphanFilesExpirationRequest] = settings.map { s =>
    JdbcOrphanFilesExpirationRequest("database", s.batchThreshold, s.retentionThreshold, batchIndex)
  }

  /** Gets the analyze request.
    *
    * @param settings
    *   The analyze settings.
    * @return
    *   The analyze request.
    */
  override def getAnalyzeRequest(settings: Option[AnalyzeSettings]): Option[JdbcAnalyzeRequest] = settings.map { s =>
    JdbcAnalyzeRequest("database", s.batchThreshold, Seq.empty[String], batchIndex)
  }
