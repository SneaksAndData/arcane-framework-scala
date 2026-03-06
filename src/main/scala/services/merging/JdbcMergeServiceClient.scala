package com.sneaksanddata.arcane.framework
package services.merging

import logging.ZIOLogAnnotations.*
import models.app.StreamContext
import models.schemas.ArcaneSchema
import models.settings.JdbcQueryRetryMode.{Always, BackfillOnly, Never}
import models.settings.backfill.BackfillSettings
import models.settings.sink.SinkSettings
import models.settings.{JdbcMergeServiceClientSettings, TablePropertiesSettings}
import services.base.*
import services.filters.FieldsFilteringService
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
    targetTableSettings: SinkSettings,
    backfillTableSettings: BackfillSettings,
    streamContext: StreamContext,
    fieldsFilteringService: FieldsFilteringService,
    tablePropertiesSettings: TablePropertiesSettings,
    declaredMetrics: DeclaredMetrics
) extends MergeServiceClient
    with JdbcTableManager
    with AutoCloseable
    with DisposeServiceClient:

  require(options.isValid, "Invalid JDBC url provided for the consumer")

  private lazy val sqlConnection: Connection = DriverManager.getConnection(options.getConnectionString)

  private val retryPolicy =
    val backoffPolicy =
      Schedule.exponential(options.queryRetryBaseDuration, options.queryRetryScaleFactor).jittered && Schedule.recurs(
        options.queryRetryMaxAttempts
      ) && Schedule.recurWhile[Throwable] {
        case _: IOException                     => true
        case _: SQLFeatureNotSupportedException => false
        case _: SQLTimeoutException             => false
        case e: SQLException => options.queryRetryOnMessageContents.exists(prefix => e.getMessage.contains(prefix))
        case _               => false
      }

    options.queryRetryMode match
      case Never                                       => Schedule.stop
      case Always                                      => backoffPolicy
      case BackfillOnly if streamContext.IsBackfilling => backoffPolicy
      case _                                           => Schedule.stop

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
      case Some(request) if request.isApplicable && !streamContext.IsBackfilling =>
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
      case Some(request) if request.isApplicable && !streamContext.IsBackfilling =>
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
      case Some(request) if request.isApplicable && !streamContext.IsBackfilling =>
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
      case Some(request) if request.isApplicable && !streamContext.IsBackfilling =>
        executeBatchQuery(
          request.toSqlExpression,
          request.name,
          "Running ANALYZE",
          _ => BatchOptimizationResult(false)
        ).gaugeDuration(declaredMetrics.targetAnalyzeDuration)
      case _ => ZIO.succeed(BatchOptimizationResult(true))

object JdbcMergeServiceClient:

  // See: https://trino.io/docs/current/connector/iceberg.html#iceberg-to-trino-type-mapping
  extension (icebergType: Type)
    private def convertType: String = icebergType.typeId() match {
      case TypeID.BOOLEAN => "BOOLEAN"
      case TypeID.INTEGER => "INTEGER"
      case TypeID.LONG    => "BIGINT"
      case TypeID.FLOAT   => "REAL"
      case TypeID.DOUBLE  => "DOUBLE"
      case TypeID.DECIMAL =>
        s"DECIMAL(${icebergType.asInstanceOf[DecimalType].precision}, ${icebergType.asInstanceOf[DecimalType].scale})"
      case TypeID.DATE => "DATE"
      case TypeID.TIME => "TIME(6)"
      case TypeID.TIMESTAMP
          if icebergType.isInstanceOf[TimestampType] && icebergType.asInstanceOf[TimestampType].shouldAdjustToUTC() =>
        "TIMESTAMP(6) WITH TIME ZONE"
      case TypeID.TIMESTAMP
          if icebergType.isInstanceOf[TimestampType] && !icebergType.asInstanceOf[TimestampType].shouldAdjustToUTC() =>
        "TIMESTAMP(6)"
      case TypeID.STRING => "VARCHAR"
      case TypeID.UUID   => "UUID"
      case TypeID.BINARY => "VARBINARY"
      case TypeID.LIST   => s"ARRAY(${icebergType.asInstanceOf[ListType].elementType().convertType})"
      // https://trino.io/docs/current/language/types.html#row
      // struct<1002: colA: optional long, 1003: colB: optional string> -> ROW(colA BIGINT, colB VARCHAR)
      // nested supported via recursion as well
      case TypeID.STRUCT =>
        s"ROW(${icebergType.asInstanceOf[StructType].fields().asScala.map(f => s"${f.name()} ${f.`type`().convertType}").mkString(",")})"
      case _ => throw new IllegalArgumentException(s"Unsupported type: $icebergType")
    }

  /** The environment type for the JdbcConsumer.
    */
  private type Environment = JdbcMergeServiceClientSettings & SinkSettings & SchemaProvider[ArcaneSchema] &
    FieldsFilteringService & TablePropertiesSettings & StreamContext & BackfillSettings & DeclaredMetrics

  /** Factory method to create JdbcConsumer.
    * @param options
    *   The options for the consumer.
    * @return
    *   The initialized JdbcConsumer instance
    */
  def apply(
      options: JdbcMergeServiceClientSettings,
      targetTableSettings: SinkSettings,
      backfillTableSettings: BackfillSettings,
      streamContext: StreamContext,
      fieldsFilteringService: FieldsFilteringService,
      tablePropertiesSettings: TablePropertiesSettings,
      declaredMetrics: DeclaredMetrics
  ): JdbcMergeServiceClient =
    new JdbcMergeServiceClient(
      options,
      targetTableSettings,
      backfillTableSettings,
      streamContext,
      fieldsFilteringService,
      tablePropertiesSettings,
      declaredMetrics
    )

  /** The ZLayer that creates the JdbcConsumer.
    */
  val layer: ZLayer[Environment, Nothing, JdbcMergeServiceClient] =
    ZLayer.scoped {
      ZIO.fromAutoCloseable {
        for
          connectionOptions       <- ZIO.service[JdbcMergeServiceClientSettings]
          targetTableSettings     <- ZIO.service[SinkSettings]
          backfillTableSettings   <- ZIO.service[BackfillSettings]
          fieldsFilteringService  <- ZIO.service[FieldsFilteringService]
          tablePropertiesSettings <- ZIO.service[TablePropertiesSettings]
          streamContext           <- ZIO.service[StreamContext]
          declaredMetrics         <- ZIO.service[DeclaredMetrics]
        yield JdbcMergeServiceClient(
          connectionOptions,
          targetTableSettings,
          backfillTableSettings,
          streamContext,
          fieldsFilteringService,
          tablePropertiesSettings,
          declaredMetrics
        )
      }
    }
