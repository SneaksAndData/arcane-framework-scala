package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors.maintenance

import logging.ZIOLogAnnotations.zlog
import models.batches.{MergeableBatch, StagedVersionedBatch}
import models.maintenance.{
  JdbcAnalyzeRequest,
  JdbcOptimizationRequest,
  JdbcOrphanFilesExpirationRequest,
  JdbcSnapshotExpirationRequest
}
import models.settings.sink.*
import models.settings.staging.JdbcMergeServiceClientSettings
import services.metrics.DeclaredMetrics
import services.streaming.base.StagedBatchProcessor

import com.sneaksanddata.arcane.framework.models.app.PluginStreamContext
import com.sneaksanddata.arcane.framework.models.settings.TableNaming.parts
import zio.stream.ZPipeline
import zio.{Cause, Ref, Task, ZIO, ZLayer}

import java.sql.{Connection, DriverManager}

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
        statement <- ZIO.fromAutoCloseable(ZIO.attempt(sqlConnection.prepareStatement(query)))
        _ <- ZIO
          .attempt(statement.execute())
          .tapError(error => zlog("Maintenance query '%s' failed", Cause.fail(error), query))
          .ignore
      yield ()
    }

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
    ZPipeline.mapZIO { batch =>
      for
        _ <- counterRef.update(_ + 1L)
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
