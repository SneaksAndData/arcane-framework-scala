package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors

import logging.ZIOLogAnnotations.*
import models.settings.*
import services.base.{ArchiveServiceClient, MergeServiceClient}
import services.merging.{JdbcMergeServiceClient, JdbcTableManager}
import services.streaming.base.StagedBatchProcessor

import com.sneaksanddata.arcane.framework.services.consumers.{ArchiveableBatch, MergeableBatch, StagedVersionedBatch}
import zio.stream.ZPipeline
import zio.{Task, ZIO, ZLayer}

/**
 * Processor that runs archivation tasks.
 */
class ArchivationProcessor(archiveServiceClient: ArchiveServiceClient,
                           jdbcMergeServiceClient: JdbcMergeServiceClient,
                           tableManager: JdbcTableManager,
                           archiveTableSettings: ArchiveTableSettings)
  extends StagedBatchProcessor:

  /**
   * Processes the incoming data.
   *
   * @return ZPipeline (stream source for the stream graph).
   */
  override def process: ZPipeline[Any, Throwable, BatchType, BatchType] =
    ZPipeline.mapZIO(batchesSet =>
      for _ <- zlog(s"Archiving batch set with index ${batchesSet.batchIndex}")
          _ <- ZIO.foreach(batchesSet.groupedBySchema)(batch => tableManager.migrateSchema(batch.schema, batch.archiveTableName))
          _ <- archiveTargets(batchesSet.groupedBySchema.groupBy(_.archiveTableName))
          _ <- ZIO.foreach(batchesSet.groupedBySchema)(batch => archiveServiceClient.disposeBatch(batch))
          _ <- runMaintenanceTasks(batchesSet, archiveTableSettings.maintenanceSettings, tableManager)
      yield batchesSet
    )
    
  private def archiveTargets(groupedByTarget: Map[String, Iterable[StagedVersionedBatch & MergeableBatch & ArchiveableBatch]]): Task[Unit] =
    ZIO.foreachDiscard(groupedByTarget) {
      case (archiveTableName, batches) =>
        for
          finalArchiveSchema <- jdbcMergeServiceClient.getSchemaProvider(archiveTableName).getSchema
          _ <- ZIO.foreachDiscard(batches)(batch => archiveServiceClient.archiveBatch(batch, finalArchiveSchema))
        yield ()
    }
    
    

object ArchivationProcessor:

  /**
   * Factory method to create ArchivationProcessor
   *
   * @param archiveServiceClient The archive service client.
   * @param jdbcMergeServiceClient The JDBC consumer.
   * @param tableManager The table manager.
   * @param archiveTableSettings The archive table settings.
   * @return The initialized ArchivationProcessor instance
   */
  def apply(archiveServiceClient: ArchiveServiceClient,
            jdbcMergeServiceClient: JdbcMergeServiceClient,
            tableManager: JdbcTableManager,
            archiveTableSettings: ArchiveTableSettings): ArchivationProcessor =
    new ArchivationProcessor(archiveServiceClient, jdbcMergeServiceClient, tableManager, archiveTableSettings)

  /**
   * The required environment for the ArchivationProcessor.
   */
  type Environment = ArchiveServiceClient & JdbcMergeServiceClient & JdbcTableManager & ArchiveTableSettings

  /**
   * The ZLayer that creates the MergeProcessor.
   */
  val layer: ZLayer[Environment, Nothing, ArchivationProcessor] =
    ZLayer {
      for
        archiveServiceClient <- ZIO.service[ArchiveServiceClient]
        jdbcMergeServiceClient <- ZIO.service[JdbcMergeServiceClient]
        tableManager <- ZIO.service[JdbcTableManager]
        archiveTableSettings <- ZIO.service[ArchiveTableSettings]
      yield ArchivationProcessor(archiveServiceClient, jdbcMergeServiceClient, tableManager, archiveTableSettings)
    }
