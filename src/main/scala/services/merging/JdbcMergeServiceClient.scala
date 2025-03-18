package com.sneaksanddata.arcane.framework
package services.merging

import logging.ZIOLogAnnotations.*

import com.sneaksanddata.arcane.framework.models.ArcaneSchema
import com.sneaksanddata.arcane.framework.models.given_NamedCell_ArcaneSchemaField
import com.sneaksanddata.arcane.framework.services.lakehouse.SchemaConversions.toIcebergSchemaFromFields

import scala.jdk.CollectionConverters.*
import services.base.*
import services.merging.models.{JdbcOptimizationRequest, JdbcOrphanFilesExpirationRequest, JdbcSnapshotExpirationRequest}
import services.merging.models.given_ConditionallyApplicable_JdbcOptimizationRequest
import services.merging.models.given_ConditionallyApplicable_JdbcSnapshotExpirationRequest
import services.merging.models.given_ConditionallyApplicable_JdbcOrphanFilesExpirationRequest
import services.merging.models.given_SqlExpressionConvertable_JdbcOptimizationRequest
import services.merging.models.given_SqlExpressionConvertable_JdbcSnapshotExpirationRequest
import services.merging.models.given_SqlExpressionConvertable_JdbcOrphanFilesExpirationRequest
import services.lakehouse.SchemaConversions
import services.merging.JdbcMergeServiceClient.{generateAlterTableSQL, readStrings}
import utils.SqlUtils.readArcaneSchema

import com.sneaksanddata.arcane.framework.models.given_CanAdd_ArcaneSchema
import com.sneaksanddata.arcane.framework.models.app.StreamContext
import com.sneaksanddata.arcane.framework.models.settings.{BackfillSettings, StagingDataSettings, TablePropertiesSettings, TargetTableSettings}
import com.sneaksanddata.arcane.framework.services.filters.FieldsFilteringService
import com.sneaksanddata.arcane.framework.services.merging.JdbcMergeServiceClient.generateCreateTableSQL
import org.apache.iceberg.Schema
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.types.Types.TimestampType
import zio.{Schedule, Task, ZIO, ZLayer}

import java.sql.{Connection, DriverManager, ResultSet}
import scala.util.Try

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

type SchemaProviderFactory = (String, Connection) => SchemaProvider[ArcaneSchema]

/**
 * A consumer that consumes batches from a JDBC source.
 *
 * @param options The options for the consumer.
 */
class JdbcMergeServiceClient(options: JdbcMergeServiceClientOptions,
                             targetTableSettings: TargetTableSettings,
                             backfillTableSettings: BackfillSettings,
                             stagingDataSettings: StagingDataSettings,
                             streamContext: StreamContext,
                             schemaProvider: SchemaProvider[ArcaneSchema],
                             fieldsFilteringService: FieldsFilteringService,
                             tablePropertiesSettings: TablePropertiesSettings,
                             schemaProviderCache: SchemaCache,
                             maybeSchemaProviderFactory: Option[SchemaProviderFactory])
  extends MergeServiceClient with JdbcTableManager with AutoCloseable with DisposeServiceClient:

  require(options.isValid, "Invalid JDBC url provided for the consumer")

  private lazy val sqlConnection: Connection = DriverManager.getConnection(options.connectionUrl)
  
  private val schemaProviderFactory = maybeSchemaProviderFactory.getOrElse((name, connection) => new JdbcSchemaProvider(name, connection))

  /**
   * @inheritdoc
   */
  override def applyBatch(batch: Batch): Task[BatchApplicationResult] =
    executeBatchQuery(batch.batchQuery.query, batch.name, "Applying", _ => true)

  /**
   * @inheritdoc
   */
  override def disposeBatch(batch: Batch): Task[BatchDisposeResult] =
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
      for targetSchema <- getSchema(tableName)
          missingFields = targetSchema.getMissingFields(newSchema)
          _ <- addColumns(tableName, missingFields)
      yield ()

  /**
   * @inheritdoc
   */
  def createTargetTable: Task[Unit] =
    for
      _ <- zlog("Creating target table", Seq(getAnnotation("targetTableName", targetTableSettings.targetTableFullName)))
      schema: ArcaneSchema <- schemaProvider.getSchema
      created <- createTable(targetTableSettings.targetTableFullName, fieldsFilteringService.filter(schema), tablePropertiesSettings)
    yield ()

  /**
   * @inheritdoc
   */
  def createBackFillTable: Task[Unit] =
    if streamContext.IsBackfilling then
      for
        _ <- zlog("Creating backfill table", Seq(getAnnotation("backfillTableName", backfillTableSettings.backfillTableFullName)))
        schema: ArcaneSchema <- schemaProvider.getSchema
        created <- createTable(backfillTableSettings.backfillTableFullName, fieldsFilteringService.filter(schema), tablePropertiesSettings)
      yield ()
    else
      ZIO.unit

  /**
   * @inheritdoc
   */
  def createStagingTable: Task[Unit] =
    for
      _ <- zlog("Creating staging table", Seq(getAnnotation("stagingTableName", stagingDataSettings.getStagingTableName)))
      schema: ArcaneSchema <- schemaProvider.getSchema
      created <- createTable(stagingDataSettings.getStagingTableName, fieldsFilteringService.filter(schema), tablePropertiesSettings)
    yield ()

  private def createTable(name: String, schema: Schema, properties: TablePropertiesSettings): Task[Unit] =
    ZIO.attemptBlocking(sqlConnection.prepareStatement(generateCreateTableSQL(name, schema, properties)).execute())

  private def dropTable(tableName: String): Task[Unit] =
    val sql = s"DROP TABLE IF EXISTS $tableName"
    ZIO.scoped {
      for statement <- ZIO.fromAutoCloseable(ZIO.attemptBlocking(sqlConnection.prepareStatement(sql)))
          _ <- zlog("Dropping table: " + tableName)
          _ <- ZIO.attemptBlocking(statement.execute())
      yield ()
    }

  def getSchema(tableName: String): Task[ArcaneSchema] =
    schemaProviderCache.getSchemaProvider(tableName, name => schemaProviderFactory(name, sqlConnection))

  private def addColumns(targetTableName: String, missingFields: ArcaneSchema): Task[Unit] =
    missingFields match
      case Nil => ZIO.unit
      case _ =>
          for _ <- ZIO.foreach(missingFields)(field => {
            val query = generateAlterTableSQL(targetTableName, field.name, SchemaConversions.toIcebergType(field.fieldType))
            zlog(s"Adding column to table $targetTableName: ${field.name} ${field.fieldType}, $query")
              *> ZIO.attemptBlocking(sqlConnection.prepareStatement(query).execute())
          })
          _ <- schemaProviderCache.refreshSchemaProvider(targetTableName, name => schemaProviderFactory(name, sqlConnection))
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

  private def readStrings(row: ResultSet): List[String] =
    Iterator.iterate(row.next())(_ => row.next())
      .takeWhile(identity)
      .map(_ => row.getString(1)).toList

  def generateAlterTableSQL(tableName: String, fieldName: String, fieldType: Type): String = s"ALTER TABLE $tableName ADD COLUMN $fieldName ${fieldType.convertType}"

  def generateCreateTableSQL(tableName: String, schema: Schema, properties: TablePropertiesSettings): String =
    val columns = schema.columns().asScala.map { field => s"${field.name()} ${field.`type`().convertType}" }.mkString(", ")
    s"CREATE TABLE IF NOT EXISTS $tableName ($columns) ${properties.serializeToWithExpression}"

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
    & TargetTableSettings
    & SchemaProvider[ArcaneSchema]
    & FieldsFilteringService
    & TablePropertiesSettings
    & StreamContext
    & BackfillSettings
    & SchemaCache
    & StagingDataSettings

  /**
   * Factory method to create JdbcConsumer.
   * @param options The options for the consumer.
   * @return The initialized JdbcConsumer instance
   */
  def apply(options: JdbcMergeServiceClientOptions,
            targetTableSettings: TargetTableSettings,
            backfillTableSettings: BackfillSettings,
            stagingDataSettings: StagingDataSettings,
            streamContext: StreamContext,
            schemaProvider: SchemaProvider[ArcaneSchema],
            fieldsFilteringService: FieldsFilteringService,
            tablePropertiesSettings: TablePropertiesSettings,
            schemaProviderManager: SchemaCache,
            maybeSchemaProviderFactory: Option[SchemaProviderFactory]): JdbcMergeServiceClient =
    new JdbcMergeServiceClient(options, targetTableSettings, backfillTableSettings, stagingDataSettings, streamContext, schemaProvider, fieldsFilteringService, tablePropertiesSettings, schemaProviderManager, maybeSchemaProviderFactory)

  /**
   * The ZLayer that creates the JdbcConsumer.
   */
  val layer: ZLayer[Environment, Nothing, JdbcMergeServiceClient] =
    ZLayer.scoped {
      ZIO.fromAutoCloseable {
        for
          connectionOptions <- ZIO.service[JdbcMergeServiceClientOptions]
          targetTableSettings <- ZIO.service[TargetTableSettings]
          backfillTableSettings <- ZIO.service[BackfillSettings]
          stagingDataSettings <- ZIO.service[StagingDataSettings]
          schemaProvider <- ZIO.service[SchemaProvider[ArcaneSchema]]
          fieldsFilteringService <- ZIO.service[FieldsFilteringService]
          tablePropertiesSettings <- ZIO.service[TablePropertiesSettings]
          streamContext <- ZIO.service[StreamContext]
          schemaProviderManager <- ZIO.service[SchemaCache]
        yield JdbcMergeServiceClient(connectionOptions, targetTableSettings, backfillTableSettings, stagingDataSettings, streamContext, schemaProvider, fieldsFilteringService, tablePropertiesSettings, schemaProviderManager, None)
      }
    }
