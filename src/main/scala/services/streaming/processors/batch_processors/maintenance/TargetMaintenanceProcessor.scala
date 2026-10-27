package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors.maintenance

import logging.ZIOLogAnnotations.{zlog, zlogWarning}
import models.app.PluginStreamContext
import models.batches.{MergeableBatch, StagedVersionedBatch}
import models.maintenance.{
  JdbcAnalyzeRequest,
  JdbcOptimizationRequest,
  JdbcOrphanFilesExpirationRequest,
  JdbcSnapshotExpirationRequest
}
import models.settings.TableNaming.parts
import models.settings.sink.*
import models.settings.staging.JdbcMergeServiceClientSettings
import services.metrics.DeclaredMetrics
import services.streaming.base.StagedBatchProcessor

import zio.stream.ZPipeline
import zio.{Cause, Ref, Task, ZIO, ZLayer}

import java.sql.{Connection, DriverManager, Statement}

class TargetMaintenanceProcessor(
    counterRef: Ref[Long],
    options: JdbcMergeServiceClientSettings,
    maintenanceSettings: TableMaintenanceSettings,
    defaultCatalogName: String,
    defaultSchemaName: String,
    declaredMetrics: DeclaredMetrics,
    isBackfilling: Boolean
) extends StagedBatchProcessor
    with AutoCloseable:
  require(options.isValid, "Invalid JDBC url provided for the consumer")

  private lazy val sqlConnection: Connection = DriverManager.getConnection(
    options.getConnectionString(defaultCatalogName, defaultSchemaName, options.credentialType)
  )

  private def executeMaintenanceQuery(query: String): Task[Unit] =
    ZIO.scoped {
      for
        _         <- zlog("Running maintenance: %s", query)
        statement <- ZIO.fromAutoCloseable(ZIO.attempt(sqlConnection.prepareStatement(query)))
        _ <- ZIO
          .attemptBlockingCancelable(statement.execute())(cancelEffect(statement, query))
          .tapError(error => zlog("Maintenance query '%s' failed", Cause.fail(error), query))
          .ignore
      yield ()
    }

  /** Cancels a running statement on the server side. This is required to avoid orphan queries being left running on
    * the query engine when the fiber executing the statement is interrupted (e.g. on pod/process
    * termination) while the underlying blocking JDBC call is still in progress.
    */
  private def cancelEffect(statement: Statement, query: String): zio.UIO[Unit] =
    zlogWarning("Cancelling in-flight maintenance query '%s' due to interruption", query) *>
      ZIO.succeed(statement.cancel()).ignore

  private def optimizeTable(tableName: String, settings: OptimizeSettings): Task[Unit] = for
    batchCount <- counterRef.get
    request <- ZIO.succeed(
      JdbcOptimizationRequest(
        tableName = tableName,
        optimizeThreshold = settings.batchThreshold,
        fileSizeThreshold = settings.fileSizeThreshold,
        batchCount = batchCount
      )
    )
    _ <- ZIO.when(request.isDefined)(executeMaintenanceQuery(request.get.toSqlExpression))
  yield ()

  private def expireOrphanFiles(tableName: String, settings: OrphanFilesExpirationSettings): Task[Unit] = for
    batchCount <- counterRef.get
    request <- ZIO.succeed(
      JdbcOrphanFilesExpirationRequest(
        tableName = tableName,
        optimizeThreshold = settings.batchThreshold,
        retentionThreshold = settings.retentionThreshold,
        batchCount = batchCount
      )
    )
    _ <- ZIO.when(request.isDefined)(executeMaintenanceQuery(request.get.toSqlExpression))
  yield ()

  private def expireSnapshots(tableName: String, settings: SnapshotExpirationSettings): Task[Unit] = for
    batchCount <- counterRef.get
    request <- ZIO.succeed(
      JdbcSnapshotExpirationRequest(
        tableName = tableName,
        optimizeThreshold = settings.batchThreshold,
        retentionThreshold = settings.retentionThreshold,
        batchCount = batchCount
      )
    )
    _ <- ZIO.when(request.isDefined)(executeMaintenanceQuery(request.get.toSqlExpression))
  yield ()

  private def analyzeTable(tableName: String, settings: AnalyzeSettings): Task[Unit] = for
    batchCount <- counterRef.get
    request <- ZIO.succeed(
      JdbcAnalyzeRequest(
        tableName = tableName,
        optimizeThreshold = settings.batchThreshold,
        includedColumns = settings.includedColumns,
        batchCount = batchCount
      )
    )
    _ <- ZIO.when(request.isDefined)(executeMaintenanceQuery(request.get.toSqlExpression))
  yield ()

  override def process
      : ZPipeline[Any, Throwable, StagedVersionedBatch & MergeableBatch, StagedVersionedBatch & MergeableBatch] =
    ZPipeline[BatchType].mapZIO { batch =>
      for
        _ <- ZIO.unless(batch.isEmpty)(counterRef.update(_ + 1L))
        _ <- optimizeTable(batch.targetTableName, maintenanceSettings.targetOptimizeSettings)
        _ <- expireSnapshots(batch.targetTableName, maintenanceSettings.targetSnapshotExpirationSettings)
        _ <- expireOrphanFiles(batch.targetTableName, maintenanceSettings.targetOrphanFilesExpirationSettings)
        _ <- ZIO.unless(isBackfilling)(analyzeTable(batch.targetTableName, maintenanceSettings.targetAnalyzeSettings))
      yield batch
    }

  override def close(): Unit = sqlConnection.close()

object TargetMaintenanceProcessor:
  def apply(
      counterRef: Ref[Long],
      options: JdbcMergeServiceClientSettings,
      maintenanceSettings: TableMaintenanceSettings,
      defaultCatalogName: String,
      defaultSchemaName: String,
      declaredMetrics: DeclaredMetrics,
      isBackfilling: Boolean
  ): TargetMaintenanceProcessor = new TargetMaintenanceProcessor(
    counterRef,
    options,
    maintenanceSettings,
    defaultCatalogName,
    defaultSchemaName,
    declaredMetrics,
    isBackfilling
  )

  val layer = ZLayer {
    for
      context     <- ZIO.service[PluginStreamContext]
      counter     <- Ref.make(0L)
      metrics     <- ZIO.service[DeclaredMetrics]
      backfilling <- context.isBackfilling
    yield TargetMaintenanceProcessor(
      counter,
      context.sink.mergeServiceClient,
      context.sink.maintenanceSettings,
      context.sink.targetTableFullName.parts.warehouse,
      context.sink.targetTableFullName.parts.namespace,
      metrics,
      backfilling
    )
  }
