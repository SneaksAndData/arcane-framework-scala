package com.sneaksanddata.arcane.framework
package services.merging

import logging.ZIOLogAnnotations.*
import models.app.PluginStreamContext
import models.settings.staging.{
  AlwaysImpl,
  BackfillOnlyImpl,
  JdbcCredentialType,
  JdbcMergeServiceClientSettings,
  NeverImpl
}
import services.base.*
import services.merging.maintenance.{*, given}
import services.metrics.DeclaredMetrics
import services.metrics.DeclaredMetrics.*

import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.types.Types.{DecimalType, ListType, StructType, TimestampType}
import zio.{Schedule, Task, ZIO, ZLayer}

import java.io.IOException
import java.sql.*
import scala.jdk.CollectionConverters.*

trait JdbcTableManager extends TableManager:
  /** @inheritdoc
    */
  override type TableOptimizationRequest = JdbcOptimizationRequest

  /** @inheritdoc
    */
  override type SnapshotExpirationRequest = JdbcSnapshotExpirationRequest

  /** @inheritdoc
    */
  override type OrphanFilesExpirationRequest = JdbcOrphanFilesExpirationRequest

  override type TableAnalyzeRequest = JdbcAnalyzeRequest

/** A consumer that consumes batches from a JDBC source.
  *
  * @param options
  *   The options for the consumer.
  */
class JdbcMergeServiceClient(
    options: JdbcMergeServiceClientSettings,
    defaultCatalogName: String,
    defaultSchemaName: String,
    declaredMetrics: DeclaredMetrics,
    isBackfilling: Boolean
) extends MergeServiceClient
    with JdbcTableManager
    with AutoCloseable
    with DisposeServiceClient:

  require(options.isValid, "Invalid JDBC url provided for the consumer")

  private lazy val sqlConnection: Connection = DriverManager.getConnection(
    options.getConnectionString(defaultCatalogName, defaultSchemaName, options.credentialType)
  )

  private val retryPolicy =
    val backoffPolicy =
      (Schedule.exponential(options.queryRetryBaseDuration, options.queryRetryScaleFactor).jittered && Schedule.recurs(
        options.queryRetryMaxAttempts
      ) && Schedule.recurWhile[Throwable] {
        case _: IOException                     => true
        case _: SQLFeatureNotSupportedException => false
        case _: SQLTimeoutException             => false
        case e: SQLException => options.queryRetryOnMessageContents.exists(prefix => e.getMessage.contains(prefix))
        case _               => false
      }).tapOutput { retryInfo =>
        zlog(
          s"Statement failed with: ${retryInfo._3.getMessage}, retry #${retryInfo._2}, retry duration ${retryInfo._1.toSeconds}s"
        )
      }

    options.queryRetryMode match
      case NeverImpl(_)                         => Schedule.stop
      case AlwaysImpl(_)                        => backoffPolicy
      case BackfillOnlyImpl(_) if isBackfilling => backoffPolicy
      case _                                    => Schedule.stop

  /** @inheritdoc
    */
  override def applyBatch(batch: Batch): Task[BatchApplicationResult] =
    executeBatchQuery(batch.batchQuery.query, batch.name, "Applying", _ => true)
      .gaugeDuration(declaredMetrics.batchMergeDuration)

  /** @inheritdoc
    */
  override def disposeBatch(batch: Batch): Task[BatchDisposeResult] =
    ZIO
      .unless(batch.isEmpty)(
        executeBatchQuery(batch.disposeExpr, batch.name, "Disposing", _ => new BatchDisposeResult)
          .gaugeDuration(declaredMetrics.batchDisposeDuration)
      )
      .map(_.getOrElse(new BatchDisposeResult))

  /** @inheritdoc
    */
  override def optimizeTable(maybeRequest: Option[TableOptimizationRequest]): Task[BatchOptimizationResult] =
    maybeRequest match
      case Some(request) if request.isApplicable && !isBackfilling =>
        executeBatchQuery(
          request.toSqlExpression,
          maybeRequest.get.name,
          "Optimizing",
          _ => BatchOptimizationResult(false)
        ).gaugeDuration(declaredMetrics.targetOptimizeDuration)
      case _ => ZIO.succeed(BatchOptimizationResult(true))

  /** @inheritdoc
    */
  override def expireSnapshots(maybeRequest: Option[SnapshotExpirationRequest]): Task[BatchOptimizationResult] =
    maybeRequest match
      case Some(request) if request.isApplicable && !isBackfilling =>
        executeBatchQuery(
          request.toSqlExpression,
          request.name,
          "Expiring old snapshots",
          _ => BatchOptimizationResult(false)
        ).gaugeDuration(declaredMetrics.targetSnapshotExpireDuration)
      case _ => ZIO.succeed(BatchOptimizationResult(true))

  /** @inheritdoc
    */
  override def expireOrphanFiles(maybeRequest: Option[OrphanFilesExpirationRequest]): Task[BatchOptimizationResult] =
    maybeRequest match
      case Some(request) if request.isApplicable && !isBackfilling =>
        executeBatchQuery(
          request.toSqlExpression,
          request.name,
          "Removing orphan files",
          _ => BatchOptimizationResult(false)
        ).gaugeDuration(declaredMetrics.targetRemoveOrphanDuration)
      case _ => ZIO.succeed(BatchOptimizationResult(true))

  /** @inheritdoc
    */
  override def close(): Unit = sqlConnection.close()

  private type ResultMapper[Result] = Boolean => Result

  private def executeBatchQuery[Result](
      query: String,
      batchName: String,
      operation: String,
      resultMapper: ResultMapper[Result]
  ): Task[Result] =
    ZIO.scoped {
      for
        statement         <- ZIO.fromAutoCloseable(ZIO.attempt(sqlConnection.prepareStatement(query)))
        _                 <- zlog(s"$operation batch $batchName")
        applicationResult <- ZIO.attempt(statement.execute()).retry(retryPolicy)
      yield resultMapper(applicationResult)
    }

  override def analyzeTable(request: Option[TableAnalyzeRequest]): Task[BatchOptimizationResult] =
    request match
      case Some(request) if request.isApplicable && !isBackfilling =>
        executeBatchQuery(
          request.toSqlExpression,
          request.name,
          "Running ANALYZE",
          _ => BatchOptimizationResult(false)
        ).gaugeDuration(declaredMetrics.targetAnalyzeDuration)
      case _ => ZIO.succeed(BatchOptimizationResult(true))

object JdbcMergeServiceClient:
  /** The environment type for the JdbcConsumer.
    */
  private type Environment = PluginStreamContext & DeclaredMetrics

  /** Factory method to create JdbcConsumer.
    * @param options
    *   The options for the consumer.
    * @return
    *   The initialized JdbcConsumer instance
    */
  def apply(
      options: JdbcMergeServiceClientSettings,
      defaultCatalogName: String,
      defaultSchemaName: String,
      isBackfilling: Boolean,
      declaredMetrics: DeclaredMetrics
  ): JdbcMergeServiceClient =
    new JdbcMergeServiceClient(
      options,
      defaultCatalogName,
      defaultSchemaName,
      declaredMetrics,
      isBackfilling
    )

  /** The ZLayer that creates the JdbcConsumer.
    */
  val layer: ZLayer[Environment, Nothing, JdbcMergeServiceClient] =
    ZLayer.scoped {
      ZIO.fromAutoCloseable {
        for
          context         <- ZIO.service[PluginStreamContext]
          declaredMetrics <- ZIO.service[DeclaredMetrics]
          isBackfilling   <- context.isBackfilling.orElseSucceed(false)
        yield JdbcMergeServiceClient(
          context.sink.mergeServiceClient,
          context.staging.table.stagingCatalogName,
          context.staging.table.stagingSchemaName,
          isBackfilling,
          declaredMetrics
        )
      }
    }
