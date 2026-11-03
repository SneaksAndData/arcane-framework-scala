package com.sneaksanddata.arcane.framework
package migrations.m1

import logging.ZIOLogAnnotations.zlog
import migrations.base.Migration
import models.app.PluginStreamContext
import models.settings.TableNaming.parts
import models.settings.sources.modification.FieldSelector
import services.base.{MergeServiceClient, StreamingSource}
import services.iceberg.base.SinkPropertyManager

import zio.ZIO

import scala.jdk.CollectionConverters.*

/** In 2.3, a bug in field selector let all fields out of source into the sink schema. This migration aligns the sink
  * schema for streams that have a field selector configured by dropping any excess columns from the sink table, while
  * always preserving essential fields such as ARCANE_MERGE_KEY and ARCANE_VERSION.
  */
class FieldSelectorSchemaMigration extends Migration:
  val description: String =
    "Aligns sink table schema with field selector rules by dropping excess columns, preserving essential fields"
  val shortName: String = "field_selector_schema"
  val uniqueId: Long    = 81920374L

  def execute(
      sinkPropertyManager: SinkPropertyManager,
      context: PluginStreamContext,
      mergeClient: MergeServiceClient,
      streamingSource: StreamingSource
  ): ZIO[Any, Throwable, Unit] =
    val targetTable     = context.sink.targetTableFullName
    val targetTableName = targetTable.parts.name
    val hasFieldSelector = context.source.modifications.rowModifications.exists {
      case _: FieldSelector => true
      case _                => false
    }

    if !hasFieldSelector then zlog("No field selector configured for %s, skipping %s migration", targetTable, shortName)
    else
      for
        targetSchema <- sinkPropertyManager.getTableSchema(targetTableName)
        targetColumns = targetSchema.columns().asScala.map(_.name()).toSet
        sourceSchema <- streamingSource.getSchema
        sourceFieldNames = sourceSchema.map(_.name.toLowerCase).toSet
        allowedColumns   = sourceFieldNames ++ streamingSource.essentialFields.map(_.toLowerCase)
        excessColumns    = targetColumns.filterNot(col => allowedColumns.contains(col.toLowerCase))
        _ <- ZIO.when(excessColumns.nonEmpty) {
          for
            _ <- zlog(
              "Dropping excess columns [%s] from %s to align with field selector",
              excessColumns.mkString(", "),
              targetTable
            )
            _ <- ZIO.foreachDiscard(excessColumns.toSeq.sorted) { col =>
              val dropQuery = s"ALTER TABLE $targetTable DROP COLUMN $col"
              for
                _ <- zlog("Executing drop column query: %s", dropQuery)
                _ <- mergeClient.execute(dropQuery, shortName, "Migrating")
                _ <- zlog("Successfully dropped column %s from %s", col, targetTable)
              yield ()
            }
          yield ()
        }
        _ <- ZIO.when(excessColumns.isEmpty) {
          zlog("Sink table %s already complies with field selector rules", targetTable)
        }
      yield ()
