package com.sneaksanddata.arcane.framework
package services.merging

import logging.ZIOLogAnnotations.*
import models.app.PluginStreamContext
import models.maintenance.{
  JdbcAnalyzeRequest,
  JdbcOptimizationRequest,
  JdbcOrphanFilesExpirationRequest,
  JdbcSnapshotExpirationRequest
}
import models.settings.staging.{AlwaysImpl, BackfillOnlyImpl, JdbcMergeServiceClientSettings, NeverImpl}
import services.base.*
import services.metrics.DeclaredMetrics
import services.metrics.DeclaredMetrics.*

import com.sneaksanddata.arcane.framework.models.batches.StagedBatch
import com.sneaksanddata.arcane.framework.models.sharding.StagedShard
import zio.{Schedule, Task, ZIO, ZLayer}

import java.io.IOException
import java.sql.*

/** Merge Service client that uses JDBC-compliant engine to perform batch merge operations.
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
    with AutoCloseable:

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
        zlogWarning(
          s"Statement failed with: ${retryInfo._3.getMessage}, retry #${retryInfo._2}, retry duration ${retryInfo._1.toMillis} ms"
        )
      }

    options.queryRetryMode match
      case NeverImpl(_)                         => Schedule.stop
      case AlwaysImpl(_)                        => backoffPolicy
      case BackfillOnlyImpl(_) if isBackfilling => backoffPolicy
      case _                                    => Schedule.stop

  /** @inheritdoc
    */
  override def applyBatch(batch: StagedBatch): Task[BatchApplicationResult] =
    executeBatchQuery(batch.batchQuery.query, batch.name, "Applying", _ => true)
      .gaugeDuration(declaredMetrics.batchMergeDuration)

  override def commitShard(shard: StagedShard): Task[ShardCommitResult] =
    executeBatchQuery(shard.commitQuery.query, shard.shardId, "Committing", _ => true)
      .gaugeDuration(declaredMetrics.shardCommitDuration)

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
