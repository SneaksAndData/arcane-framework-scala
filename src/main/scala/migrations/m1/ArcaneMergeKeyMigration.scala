package com.sneaksanddata.arcane.framework
package migrations.m1

import exceptions.FatalStreamFailException
import logging.ZIOLogAnnotations.zlog
import migrations.base.Migration
import models.app.PluginStreamContext
import models.schemas.MergeKeyField
import models.settings.TableNaming.parts
import models.settings.mssql.MsSqlServerDatabaseSourceSettings
import services.iceberg.base.SinkPropertyManager
import services.merging.JdbcMergeServiceClient
import services.mssql.QueryProvider
import services.mssql.SqlDataCell.normalizeName

import com.microsoft.sqlserver.jdbc.SQLServerDriver
import zio.{Task, ZIO}

import java.sql.ResultSet
import java.util.Properties
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.*

class ArcaneMergeKeyMigration extends Migration:
  val description: String =
    "Updates ARCANE_MERGE_KEY column values for SQL Server source using unified murmur3 computation"
  val shortName: String = "arcane_merge_key"
  val uniqueId: Long    = 84729104L

  private val driver = new SQLServerDriver()

  @tailrec
  private def readPrimaryKeys(resultSet: ResultSet, acc: List[String] = Nil): List[String] =
    if !resultSet.next() then acc
    else
      val colName = resultSet.getString(1)
      val isPk    = resultSet.getInt(2) == 1
      val nextAcc = if isPk then acc :+ colName.normalizeName else acc
      readPrimaryKeys(resultSet, nextAcc)

  private def getPrimaryKeys(settings: MsSqlServerDatabaseSourceSettings): Task[List[String]] =
    ZIO.scoped {
      for
        connection <- ZIO.fromAutoCloseable(
          ZIO.attemptBlocking(driver.connect(settings.getConnectionString, new Properties()))
        )
        catalog = connection.getCatalog
        query     <- QueryProvider.getColumnSummariesQuery(settings.schemaName, settings.tableName, catalog)
        statement <- ZIO.fromAutoCloseable(ZIO.attemptBlocking(connection.createStatement()))
        resultSet <- ZIO.acquireRelease(ZIO.attemptBlocking(statement.executeQuery(query))) { rs =>
          ZIO.succeed(statement.cancel()) *> ZIO.attemptBlocking(rs.close()).orDie
        }
        primaryKeys <- ZIO.attemptBlocking(readPrimaryKeys(resultSet))
      yield primaryKeys
    }

  def execute(
      sinkPropertyManager: SinkPropertyManager,
      context: PluginStreamContext,
      mergeClient: JdbcMergeServiceClient
  ): ZIO[Any, Throwable, Unit] = context.source.configuration match
    case sqlServerSettings: MsSqlServerDatabaseSourceSettings =>
      for
        targetSchema <- sinkPropertyManager.getTableSchema(context.sink.targetTableFullName.parts.name)
        hasMergeKey = targetSchema.columns().asScala.exists(_.name().equalsIgnoreCase(MergeKeyField.name))
        _ <- ZIO.unless(hasMergeKey)(
          ZIO.fail(
            FatalStreamFailException(
              s"Target table ${context.sink.targetTableFullName} does not contain ${MergeKeyField.name} column"
            )
          )
        )
        primaryKeys <- getPrimaryKeys(sqlServerSettings)
        _ <- ZIO.when(primaryKeys.nonEmpty) {
          val concatExpression = primaryKeys.map(pk => s"cast($pk as varchar)").mkString(" || '#' || ")
          val mergeKeyExpr     = s"lower(to_hex(murmur3(to_utf8($concatExpression))))"
          val updateQuery =
            s"UPDATE ${context.sink.targetTableFullName} SET ${MergeKeyField.name} = $mergeKeyExpr"

          for
            _ <- zlog("Executing ARCANE_MERGE_KEY migration: %s", updateQuery)
            _ <- mergeClient.execute(updateQuery, shortName, "Migrating")
            _ <- zlog("Successfully migrated ARCANE_MERGE_KEY for %s", context.sink.targetTableFullName)
          yield ()
        }
        _ <- ZIO.when(primaryKeys.isEmpty) {
          zlog(
            "No primary keys found for SQL Server table %s.%s, skipping ARCANE_MERGE_KEY migration",
            sqlServerSettings.schemaName,
            sqlServerSettings.tableName
          )
        }
      yield ()

    case _ =>
      zlog("Skipping ARCANE_MERGE_KEY migration: source is not SQL Server")
