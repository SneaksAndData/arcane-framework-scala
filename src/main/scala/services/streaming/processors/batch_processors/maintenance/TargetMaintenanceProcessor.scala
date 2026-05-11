package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors.maintenance

import models.batches.{MergeableBatch, StagedVersionedBatch}
import models.maintenance.{JdbcAnalyzeRequest, JdbcOptimizationRequest, JdbcOrphanFilesExpirationRequest, JdbcSnapshotExpirationRequest}
import models.settings.sink.{AnalyzeSettings, OptimizeSettings, OrphanFilesExpirationSettings, SnapshotExpirationSettings, TableMaintenanceSettings}
import models.settings.staging.JdbcMergeServiceClientSettings
import services.metrics.DeclaredMetrics
import services.streaming.base.StagedBatchProcessor

import com.sneaksanddata.arcane.framework.services.base.BatchOptimizationResult
import zio.stream.ZPipeline
import zio.{Cached, Ref, Task, ZIO}

import java.sql.{Connection, DriverManager}

class TargetMaintenanceProcessor(counterRef: Ref[Long],     
                                 options: JdbcMergeServiceClientSettings,
                                 maintenanceSettings: TableMaintenanceSettings,
                                 defaultCatalogName: String,
                                 defaultSchemaName: String,
                                 declaredMetrics: DeclaredMetrics,
                                 isBackfilling: Boolean) extends StagedBatchProcessor with AutoCloseable:
  require(options.isValid, "Invalid JDBC url provided for the consumer")

  private lazy val sqlConnection: Connection = DriverManager.getConnection(
    options.getConnectionString(defaultCatalogName, defaultSchemaName, options.credentialType)
  )

  // TODO: ignore maintenance failures
  private def executeMaintenanceQuery(query: String): Task[Unit] =
    ZIO.scoped {
      for
        statement <- ZIO.fromAutoCloseable(ZIO.attempt(sqlConnection.prepareStatement(query)))
        _ <- ZIO.attempt(statement.execute()) //.retry(retryPolicy)
      yield ()
    }
  
  private def optimizeTable(tableName: String, settings: OptimizeSettings): Task[BatchOptimizationResult] = for
    batchCount <- counterRef.get
    request <- ZIO.succeed(JdbcOptimizationRequest(
      tableName = tableName,
      optimizeThreshold = settings.batchThreshold,
      fileSizeThreshold = settings.fileSizeThreshold, 
      batchCount = batchCount
    ))
    resultExecuted <- ZIO.when(request.isDefined)(executeMaintenanceQuery(request.get.toSqlExpression)).map(v => v.map(_ => BatchOptimizationResult(false)))
    resultSkipped <- ZIO.when(request.isEmpty)(executeMaintenanceQuery(request.get.toSqlExpression)).map(v => v.map(_ => BatchOptimizationResult(true)))
  yield resultExecuted match
    case Some(executed) => executed
    case None => resultSkipped.get

  private def expireOrphanFiles(tableName: String, settings: OrphanFilesExpirationSettings): Task[BatchOptimizationResult] = for
    batchCount <- counterRef.get
    request <- ZIO.succeed(JdbcOrphanFilesExpirationRequest(
      tableName = tableName,
      optimizeThreshold = settings.batchThreshold,
      retentionThreshold = settings.retentionThreshold,
      batchCount = batchCount
    ))
    resultExecuted <- ZIO.when(request.isDefined)(executeMaintenanceQuery(request.get.toSqlExpression)).map(v => v.map(_ => BatchOptimizationResult(false)))
    resultSkipped <- ZIO.when(request.isEmpty)(executeMaintenanceQuery(request.get.toSqlExpression)).map(v => v.map(_ => BatchOptimizationResult(true)))
  yield resultExecuted match
    case Some(executed) => executed
    case None => resultSkipped.get


  private def expireSnapshots(tableName: String, settings: SnapshotExpirationSettings): Task[BatchOptimizationResult] = for
    batchCount <- counterRef.get
    request <- ZIO.succeed(JdbcSnapshotExpirationRequest(
      tableName = tableName,
      optimizeThreshold = settings.batchThreshold,
      retentionThreshold = settings.retentionThreshold,
      batchCount = batchCount
    ))
    resultExecuted <- ZIO.when(request.isDefined)(executeMaintenanceQuery(request.get.toSqlExpression)).map(v => v.map(_ => BatchOptimizationResult(false)))
    resultSkipped <- ZIO.when(request.isEmpty)(executeMaintenanceQuery(request.get.toSqlExpression)).map(v => v.map(_ => BatchOptimizationResult(true)))
  yield resultExecuted match
    case Some(executed) => executed
    case None => resultSkipped.get


  private def analyzeTable(tableName: String, settings: AnalyzeSettings): Task[BatchOptimizationResult] = for
    batchCount <- counterRef.get
    request <- ZIO.succeed(JdbcAnalyzeRequest(
      tableName = tableName,
      optimizeThreshold = settings.batchThreshold,
      includedColumns = settings.includedColumns,
      batchCount = batchCount
    ))
    resultExecuted <- ZIO.when(request.isDefined)(executeMaintenanceQuery(request.get.toSqlExpression)).map(v => v.map(_ => BatchOptimizationResult(false)))
    resultSkipped <- ZIO.when(request.isEmpty)(executeMaintenanceQuery(request.get.toSqlExpression)).map(v => v.map(_ => BatchOptimizationResult(true)))
  yield resultExecuted match
    case Some(executed) => executed
    case None => resultSkipped.get



  override def process: ZPipeline[Any, Throwable, StagedVersionedBatch & MergeableBatch, StagedVersionedBatch & MergeableBatch] = ZPipeline.mapZIO { batch =>
    for
      _ <- counterRef.update(_ + 1L)
      
    yield batch
  }

  override def close(): Unit = sqlConnection.close()
