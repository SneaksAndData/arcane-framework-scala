package com.sneaksanddata.arcane.framework
package services.merging

import logging.ZIOLogAnnotations.*
import models.ArcaneSchema
import models.querygen.{MergeQuery, OverwriteQuery, StreamingBatchQuery}
import services.base.{BatchApplicationResult, BatchArchivationResult, BatchDisposeResult, MergeServiceClient, TableManager}

import zio.{Schedule, Task, ZIO, ZLayer}

import java.sql.{Connection, DriverManager, ResultSet}
import java.time.Duration
import scala.concurrent.Future
import scala.util.Try;

case class JdbcOptimizationRequest(tableName: String, optimizeThreshold: Long, fileSizeThreshold: String)

case class JdbcSnapshotExpirationRequest(tableName: String, optimizeThreshold: Long, fileSizeThreshold: String)

case class JdbcOrphanFilesExpirationRequest(tableName: String, optimizeThreshold: Long, fileSizeThreshold: String)


trait JdbcConsumerOptions:
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
class JdbcMergeServiceClient(options: JdbcConsumerOptions)
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

  type BatchOptimizationRequest = JdbcOptimizationRequest

  type SnapshotExpirationRequest = JdbcSnapshotExpirationRequest

  type OrphanFilesExpirationRequest = JdbcOrphanFilesExpirationRequest

  private type ResultMapper[Result] = Boolean => Result

  private def executeBatchQuery[Result](query: String, batchName: String, operation: String, resultMapper: ResultMapper[Result]): Task[Result] =
    val statement = ZIO.attemptBlocking(sqlConnection.prepareStatement(query))
    ZIO.acquireReleaseWith(statement)(st => ZIO.succeed(st.close())){ statement =>
      for
        _ <- zlog(s"$operation batch $batchName")
        applicationResult <- ZIO.attemptBlocking(statement.execute())
      yield resultMapper(applicationResult)
    }


  override def optimizeTable(batchOptimizationRequest: BatchOptimizationRequest): Task[BatchApplicationResult] =
    if (batchNumber+1) % optimizeThreshold == 0 then
      val query = ZIO.attemptBlocking {
        sqlConnection.prepareStatement(s"ALTER TABLE $tableName execute optimize(file_size_threshold => '$fileSizeThreshold')")
      }
      ZIO.acquireReleaseWith(query)(st => ZIO.succeed(st.close())) { statement =>
        for
          _ <- zlog(s"Optimizing table $tableName. Batch number: $batchNumber. fileSizeThreshold: $fileSizeThreshold")
          _ <- ZIO.attemptBlocking { statement.execute() }
        yield true
      }
    else
      ZIO.succeed(false)

  def expireSnapshots(tableName: String, batchNumber: Long, optimizeThreshold: Long, retentionThreshold: String): Task[BatchApplicationResult] =
    if (batchNumber+1) % optimizeThreshold == 0 then
      val query = ZIO.attemptBlocking {
        sqlConnection.prepareStatement(s"ALTER TABLE $tableName execute expire_snapshots(retention_threshold => '$retentionThreshold')")
      }
      ZIO.acquireReleaseWith(query)(st => ZIO.succeed(st.close())) { statement =>
        for
          _ <- zlog(s"Run expire_snapshots for table $tableName. Batch number: $batchNumber. retentionThreshold: $retentionThreshold")
          _ <- ZIO.attemptBlocking { statement.execute() }
        yield true
      }
    else
      ZIO.succeed(false)

  def expireOrphanFiles(tableName: String, batchNumber: Long, optimizeThreshold: Long, retentionThreshold: String): Task[BatchApplicationResult] =
    if (batchNumber+1) % optimizeThreshold == 0 then
      val query = ZIO.attemptBlocking {
        sqlConnection.prepareStatement(s"ALTER TABLE $tableName execute remove_orphan_files(retention_threshold => '$retentionThreshold')")
      }
      ZIO.acquireReleaseWith(query)(st => ZIO.succeed(st.close())) { statement =>
        for
          _ <- zlog(s"Run remove_orphan_files for table $tableName. Batch number: $batchNumber. retentionThreshold: $retentionThreshold")
          _ <- ZIO.attemptBlocking { statement.execute() }
        yield true
      }
    else
      ZIO.succeed(false)

  def close(): Unit = sqlConnection.close()

  private def collectPartitionColumn(resultSet: ResultSet, columnName: String): Seq[String] =
    // do not fail on closed result sets
    if resultSet.isClosed then
      Seq.empty
    else
      val current = resultSet.getString(columnName)
      if resultSet.next() then
        collectPartitionColumn(resultSet, columnName) :+ current
      else
        resultSet.close()
        Seq(current)


object JdbcMergeServiceClient:
  /**
   * Factory method to create JdbcConsumer.
   * @param options The options for the consumer.
   * @return The initialized JdbcConsumer instance
   */
  def apply[Query <: StreamingBatchQuery](options: JdbcConsumerOptions, archiveTableSettings: ArchiveTableSettings): JdbcMergeServiceClient =
    new JdbcMergeServiceClient(options, archiveTableSettings)

  /**
   * The ZLayer that creates the JdbcConsumer.
   */
  val layer: ZLayer[Environment, Nothing, JdbcMergeServiceClient] =
    ZLayer.scoped {
      ZIO.fromAutoCloseable {
        for
          connectionOptions <- ZIO.service[JdbcConsumerOptions]
          archiveTableSettings <- ZIO.service[ArchiveTableSettings]
        yield JdbcConsumer(connectionOptions, archiveTableSettings)
      }
    }

