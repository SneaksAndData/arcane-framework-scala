package com.sneaksanddata.arcane.framework
package services.mssql.migrations

import exceptions.FatalStreamFailException
import logging.ZIOLogAnnotations.zlog
import migrations.base.Migration
import models.app.PluginStreamContext
import models.schemas.MergeKeyField
import models.settings.TableNaming.parts
import services.base.{MergeServiceClient, StreamingSource}
import services.iceberg.base.SinkPropertyManager
import services.mssql.SqlDataCell.normalizeName
import services.mssql.base.MsSqlStreamingSource

import zio.ZIO

import scala.jdk.CollectionConverters.*

class SqlServerArcaneMergeKeyMigration extends Migration:
  val description: String =
    "Updates ARCANE_MERGE_KEY column values for SQL Server source using unified murmur3 computation"
  val shortName: String = "arcane_merge_key"
  val uniqueId: Long    = 84729104L

  def execute(
      sinkPropertyManager: SinkPropertyManager,
      context: PluginStreamContext,
      mergeClient: MergeServiceClient,
      streamingSource: StreamingSource
  ): ZIO[Any, Throwable, Unit] =
    val sqlSource = streamingSource.asInstanceOf[MsSqlStreamingSource]
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
      primaryKeys      = sqlSource.columnSummaries.collect { case (name, true) => name.normalizeName }
      concatExpression = primaryKeys.map(pk => s"cast($pk as varchar)").mkString(" || '#' || ")
      mergeKeyExpr     = s"lower(to_hex(murmur3(to_utf8($concatExpression))))"
      updateQuery =
        s"UPDATE ${context.sink.targetTableFullName} SET ${MergeKeyField.name} = $mergeKeyExpr"

      _ <- zlog("Executing ARCANE_MERGE_KEY migration: %s", updateQuery)
      _ <- mergeClient.execute(updateQuery, shortName, "Migrating")
      _ <- zlog("Successfully migrated ARCANE_MERGE_KEY for %s", context.sink.targetTableFullName)
    yield ()
