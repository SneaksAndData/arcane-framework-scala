package com.sneaksanddata.arcane.framework
package services.merging

import logging.ZIOLogAnnotations.*
import com.sneaksanddata.arcane.framework.models.ArcaneSchema
import services.base.*
import services.merging.models.{JdbcOptimizationRequest, JdbcOrphanFilesExpirationRequest, JdbcSnapshotExpirationRequest}
import services.merging.models.given_ConditionallyApplicable_JdbcOptimizationRequest
import services.merging.models.given_ConditionallyApplicable_JdbcSnapshotExpirationRequest
import services.merging.models.given_ConditionallyApplicable_JdbcOrphanFilesExpirationRequest
import services.merging.models.given_SqlExpressionConvertable_JdbcOptimizationRequest
import services.merging.models.given_SqlExpressionConvertable_JdbcSnapshotExpirationRequest
import services.merging.models.given_SqlExpressionConvertable_JdbcOrphanFilesExpirationRequest


import zio.{Schedule, Task, ZIO, ZLayer}

import java.sql.{Connection, DriverManager, ResultSet}
import java.time.Duration
import scala.concurrent.Future
import scala.util.Try;

trait JdbcMergeServiceClientOptions:
  /**
   * The connection URL.
   */
  val connectionUrl: String

  /**
   * Checks if the connection URL is valid.
   *
   * @return True if the connection URL is valid, false otherwise.
   */
  final def isValid: Boolean = Try(DriverManager.getDriver(connectionUrl)).isSuccess

/**
 * A consumer that consumes batches from a JDBC source.
 *
 * @param options The options for the consumer.
 */
class JdbcMergeServiceClient(options: JdbcMergeServiceClientOptions)
  extends MergeServiceClient with TableManager with AutoCloseable:

  require(options.isValid, "Invalid JDBC url provided for the consumer")

  private lazy val sqlConnection: Connection = DriverManager.getConnection(options.connectionUrl)

  /**
   * @inheritdoc
   */
  override def applyBatch(batch: Batch): Task[BatchApplicationResult] =
    executeBatchQuery(batch.batchQuery.query, batch.name, "Applying", _ => true)

  /**
   * @inheritdoc
   */
  override def archiveBatch(batch: Batch, actualSchema: ArcaneSchema): Task[BatchArchivationResult] =
    executeBatchQuery(batch.archiveExpr(actualSchema), batch.name, "Archiving", _ => new BatchArchivationResult)

  /**
   * @inheritdoc
   */
  def disposeBatch(batch: Batch): Task[BatchDisposeResult] =
    executeBatchQuery(batch.disposeExpr, batch.name, "Disposing", _ => new BatchDisposeResult)


  /**
   * @inheritdoc
   */
  override type TableOptimizationRequest = JdbcOptimizationRequest

  /**
   * @inheritdoc
   */
  override type SnapshotExpirationRequest = JdbcSnapshotExpirationRequest

  /**
   * @inheritdoc
   */
  override type OrphanFilesExpirationRequest = JdbcOrphanFilesExpirationRequest

  /**
   * @inheritdoc
   */
  override def optimizeTable(request: TableOptimizationRequest): Task[BatchOptimizationResult] =
    if request.isApplicable then
      executeBatchQuery(request.toSqlExpression, request.name, "Archiving", _ => BatchOptimizationResult())
    else
      ZIO.succeed(BatchOptimizationResult())

  /**
   * @inheritdoc
   */
  override def expireSnapshots(request: SnapshotExpirationRequest): Task[BatchOptimizationResult] =
    if request.isApplicable then
      executeBatchQuery(request.toSqlExpression, request.name, "Running Snapshot Expiration task", _ => BatchOptimizationResult())
    else
      ZIO.succeed(BatchOptimizationResult())

  /**
   * @inheritdoc
   */
  override def expireOrphanFiles(request: OrphanFilesExpirationRequest): Task[BatchOptimizationResult] =
    if request.isApplicable then
      executeBatchQuery(request.toSqlExpression, request.name, "Removing orphan files", _ => BatchOptimizationResult())
    else
      ZIO.succeed(BatchOptimizationResult())

  /**
   * @inheritdoc
   */
  override def close(): Unit = sqlConnection.close()

  private type ResultMapper[Result] = Boolean => Result

  private def executeBatchQuery[Result](query: String, batchName: String, operation: String, resultMapper: ResultMapper[Result]): Task[Result] =
    val statement = ZIO.attemptBlocking(sqlConnection.prepareStatement(query))
    ZIO.acquireReleaseWith(statement)(st => ZIO.succeed(st.close())){ statement =>
      for
        _ <- zlog(s"$operation batch $batchName")
        applicationResult <- ZIO.attemptBlocking(statement.execute())
      yield resultMapper(applicationResult)
    }

object JdbcMergeServiceClient:

  /**
   * The environment type for the JdbcConsumer.
   */
  private type Environment = JdbcMergeServiceClientOptions
  
  /**
   * Factory method to create JdbcConsumer.
   * @param options The options for the consumer.
   * @return The initialized JdbcConsumer instance
   */
  def apply(options: JdbcMergeServiceClientOptions): JdbcMergeServiceClient =
    new JdbcMergeServiceClient(options)

  /**
   * The ZLayer that creates the JdbcConsumer.
   */
  val layer: ZLayer[Environment, Nothing, JdbcMergeServiceClient] =
    ZLayer.scoped {
      ZIO.fromAutoCloseable {
        for
          connectionOptions <- ZIO.service[JdbcMergeServiceClientOptions]
        yield JdbcMergeServiceClient(connectionOptions)
      }
    }

