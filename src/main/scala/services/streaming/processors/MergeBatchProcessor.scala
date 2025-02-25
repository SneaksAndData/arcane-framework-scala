package com.sneaksanddata.arcane.framework
package services.streaming.processors

import logging.ZIOLogAnnotations.*
import models.ArcaneSchema
import models.app.*
import models.querygen.{MergeQuery, OverwriteQuery}
import services.clients.{BatchApplicationResult, JdbcConsumer}
import services.consumers.StagedVersionedBatch
import services.streaming.base.BatchProcessor
import services.streaming.consumers.InFlightBatch

import com.sneaksanddata.arcane.microsoft_synapse_link.services.app.TableManager
import zio.stream.ZPipeline
import zio.{Task, ZIO, ZLayer}

/**
 * Processor that merges data into a target table.
 *
 * @param jdbcConsumer The JDBC consumer.
 */
class MergeBatchProcessor(mergeServiceClient: MergeServiceClient, tableManager: TableManager)
  extends BatchProcessor[InFlightBatch, InFlightBatch]:

  /**
   * Processes the incoming data.
   *
   * @return ZPipeline (stream source for the stream graph).
   */
  override def process: ZPipeline[Any, Throwable, InFlightBatch, InFlightBatch] =
    ZPipeline.mapZIO({
      case ((batches, other), batchNumber) =>
        for _ <- zlog(s"Applying batch $batchNumber")
        
            _ <- ZIO.foreach(batches)(batch => tableManager.migrateSchema(batch.schema, targetTableSettings.targetTableFullName))
            _ <- ZIO.foreach(batches)(batch => jdbcConsumer.applyBatch(batch))
        
            _ <- tryOptimizeTarget(targetTableSettings.targetTableFullName, batchNumber,
                  targetTableSettings.targetOptimizeSettings)
        
            _ <- tryExpireSnapshots(targetTableSettings.targetTableFullName, batchNumber,
                  targetTableSettings.targetSnapshotExpirationSettings)
        
            _ <- tryExpireOrphanFiles(targetTableSettings.targetTableFullName, batchNumber,
                targetTableSettings.targetOrphanFilesExpirationSettings)
          
        yield ((batches, other), batchNumber)
    })

  private def tryOptimizeTarget(targetTableFullName: String, batchNumber: Long, settings: Option[OptimizeSettings]): Task[BatchApplicationResult] =
    settings match
      case Some(settings) =>
        jdbcConsumer.optimizeTarget(targetTableFullName, batchNumber, settings.batchThreshold, settings.fileSizeThreshold)
      case None => ZIO.attempt(false)

  private def tryExpireSnapshots(targetTableFullName: String, batchNumber: Long, settings: Option[SnapshotExpirationSettings]): Task[BatchApplicationResult] =
    settings match
      case Some(settings) =>
        jdbcConsumer.expireSnapshots(targetTableFullName, batchNumber, settings.batchThreshold, settings.retentionThreshold)
      case None => ZIO.attempt(false)

  private def tryExpireOrphanFiles(targetTableFullName: String, batchNumber: Long, settings: Option[OrphanFilesExpirationSettings]): Task[BatchApplicationResult] =
    settings match
      case Some(settings) =>
        jdbcConsumer.expireOrphanFiles(targetTableFullName, batchNumber, settings.batchThreshold, settings.retentionThreshold)
      case None => ZIO.attempt(false)

object MergeBatchProcessor:

  /**
   * Factory method to create MergeProcessor
   * @param jdbcConsumer The JDBC consumer.
   * @return The initialized MergeProcessor instance
   */
  def apply(jdbcConsumer: JdbcConsumer, parallelismSettings: ParallelismSettings, targetTableSettings: TargetTableSettings, tableManager: TableManager): MergeBatchProcessor =
    new MergeBatchProcessor(jdbcConsumer, parallelismSettings, targetTableSettings, tableManager)

  /**
   * The required environment for the MergeProcessor.
   */
  type Environment = JdbcConsumer
    & ParallelismSettings
    & TargetTableSettings
    & TableManager

  /**
   * The ZLayer that creates the MergeProcessor.
   */
  val layer: ZLayer[Environment, Nothing, MergeBatchProcessor] =
    ZLayer {
      for
        jdbcConsumer <- ZIO.service[JdbcConsumer]
        parallelismSettings <- ZIO.service[ParallelismSettings]
        targetTableSettings <- ZIO.service[TargetTableSettings]
        tableManager <- ZIO.service[TableManager]
      yield MergeBatchProcessor(jdbcConsumer, parallelismSettings, targetTableSettings, tableManager)
    }
