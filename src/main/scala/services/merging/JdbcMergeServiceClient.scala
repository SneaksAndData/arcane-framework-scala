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

import com.sneaksanddata.arcane.framework.services.lakehouse.SchemaConversions
import com.sneaksanddata.arcane.framework.services.merging.JdbcMergeServiceClient.generateAlterTableSQL
import com.sneaksanddata.arcane.framework.utils.SqlUtils.readArcaneSchema
import services.mssql.given_CanAdd_ArcaneSchema
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.types.Types.TimestampType
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
  
trait JdbcTableManager extends TableManager:
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
 * A consumer that consumes batches from a JDBC source.
 *
 * @param options The options for the consumer.
 */
class JdbcMergeServiceClient(options: JdbcMergeServiceClientOptions)
  extends MergeServiceClient with JdbcTableManager with AutoCloseable:

  class JdbcSchemaProvider(tableName: String, sqlConnection: Connection) extends SchemaProvider[ArcaneSchema]:
    /**
     * @inheritdoc
     */
    override def getSchema: Task[ArcaneSchema] =
      val query = s"SELECT * FROM $tableName where true and false"
      val ack = ZIO.attemptBlocking(sqlConnection.prepareStatement(query))
      ZIO.acquireReleaseWith(ack)(st => ZIO.succeed(st.close())) { statement =>
        for
          schemaResult <- ZIO.attemptBlocking(statement.executeQuery())
          tryFields <- ZIO.attemptBlocking(schemaResult.readArcaneSchema)
          fields <- ZIO.fromTry(tryFields)
        yield fields
      }

    def empty: ArcaneSchema = ArcaneSchema.empty()


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
  override def optimizeTable(request: TableOptimizationRequest): Task[BatchOptimizationResult] =
    if request.isApplicable then
      executeBatchQuery(request.toSqlExpression, request.name, "Optimizing", _ => BatchOptimizationResult(false))
    else
      ZIO.succeed(BatchOptimizationResult(true))

  /**
   * @inheritdoc
   */
  override def expireSnapshots(request: SnapshotExpirationRequest): Task[BatchOptimizationResult] =
    if request.isApplicable then
      executeBatchQuery(request.toSqlExpression, request.name, "Running Snapshot Expiration task", _ => BatchOptimizationResult(false))
    else
      ZIO.succeed(BatchOptimizationResult(true))

  /**
   * @inheritdoc
   */
  override def expireOrphanFiles(request: OrphanFilesExpirationRequest): Task[BatchOptimizationResult] =
    if request.isApplicable then
      executeBatchQuery(request.toSqlExpression, request.name, "Removing orphan files", _ => BatchOptimizationResult(false))
    else
      ZIO.succeed(BatchOptimizationResult(true))

  /**
   * @inheritdoc
   */
  def migrateSchema(newSchema: ArcaneSchema, tableName: String): Task[Unit] =
      for targetSchema <- getSchemaProvider(tableName).getSchema
          missingFields = targetSchema.getMissingFields(newSchema)
          _ <- addColumns(tableName, missingFields)
      yield ()

  def getSchemaProvider(tableName: String): JdbcSchemaProvider = this.JdbcSchemaProvider(tableName, sqlConnection)

  private def addColumns(targetTableName: String, missingFields: ArcaneSchema): Task[Unit] =
    for _ <- ZIO.foreach(missingFields)(field => {
      val query = generateAlterTableSQL(targetTableName, field.name, SchemaConversions.toIcebergType(field.fieldType))
      zlog(s"Adding column to table $targetTableName: ${field.name} ${field.fieldType}, $query")
        *> ZIO.attemptBlocking(sqlConnection.prepareStatement(query).execute())
    })
    yield ()

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

  def generateAlterTableSQL(tableName: String, fieldName: String, fieldType: Type): String = s"ALTER TABLE $tableName ADD COLUMN $fieldName ${fieldType.convertType}"

  // See: https://trino.io/docs/current/connector/iceberg.html#iceberg-to-trino-type-mapping
  extension (icebergType: Type) private def convertType: String = icebergType.typeId() match {
    case TypeID.BOOLEAN => "BOOLEAN"
    case TypeID.INTEGER => "INTEGER"
    case TypeID.LONG => "BIGINT"
    case TypeID.FLOAT => "REAL"
    case TypeID.DOUBLE => "DOUBLE"
    case TypeID.DECIMAL => "DECIMAL(1, 2)"
    case TypeID.DATE => "DATE"
    case TypeID.TIME => "TIME(6)"
    case TypeID.TIMESTAMP if icebergType.isInstanceOf[TimestampType] && icebergType.asInstanceOf[TimestampType].shouldAdjustToUTC() => "TIMESTAMP(6) WITH TIME ZONE"
    case TypeID.TIMESTAMP if icebergType.isInstanceOf[TimestampType] && !icebergType.asInstanceOf[TimestampType].shouldAdjustToUTC() => "TIMESTAMP(6)"
    case TypeID.STRING => "VARCHAR"
    case TypeID.UUID => "UUID"
    case TypeID.BINARY => "VARBINARY"
    case _ => throw new IllegalArgumentException(s"Unsupported type: $icebergType")
  }

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
