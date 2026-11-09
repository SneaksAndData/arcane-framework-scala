package com.sneaksanddata.arcane.framework
package migrations.base

import logging.ZIOLogAnnotations.zlog
import models.MetadataKeys
import models.schemas.MergeKeyField
import models.settings.TableNaming.parts
import services.base.MergeServiceClient
import services.iceberg.base.SinkPropertyManager

import zio.ZIO

/** Rebuilds the merge key column under the framework's current name.
  *
  * Tables created before 2.4 declare the column as `ARCANE_MERGE_KEY`. Trino folds column names to lower case, so the
  * column cannot be renamed or added next to the old one under the new name. The rebuild takes four steps:
  *   1. compute the new values into a temporary column
  *   1. drop the old column
  *   1. create the column under [[MergeKeyField.name]] and copy the values from the temporary column
  *   1. drop the temporary column
  *
  * The rebuild is not atomic. The number of completed steps is recorded in a sink table property, so a run that stopped
  * halfway continues with the first step that did not complete. The statements of every step can be repeated, since a
  * run may stop between a step and the update of the property.
  */
object MergeKeyColumnRebuild:
  private val tmpColumn = s"${MergeKeyField.name}_tmp"

  /** Sink table property holding the number of completed steps. Present only while a rebuild is unfinished. */
  val progressProperty: String = MetadataKeys.getFrameworkKey(MetadataKeys.mergeKeyRebuildStepKey)

  /** @param targetTable
    *   Full name of the sink table
    * @param mergeKeyExpr
    *   Expression computing the merge key value of a row. It may reference the old merge key column, which is still
    *   present whenever the expression is evaluated.
    * @param sinkPropertyManager
    *   Used to record the progress of the rebuild on the sink table
    * @param mergeClient
    *   Client used to run the statements
    * @param migrationName
    *   Name of the calling migration, reported with each statement
    */
  def apply(
      targetTable: String,
      mergeKeyExpr: String,
      sinkPropertyManager: SinkPropertyManager,
      mergeClient: MergeServiceClient,
      migrationName: String
  ): ZIO[Any, Throwable, Unit] =
    val tableName = targetTable.parts.name

    def run(statement: String): ZIO[Any, Throwable, Unit] =
      zlog("Executing %s migration: %s", MergeKeyField.name, statement) *>
        mergeClient.execute(statement, migrationName, "Migrating")

    val steps: Seq[ZIO[Any, Throwable, Unit]] = Seq(
      // 1. compute the new values into the temporary column
      run(s"ALTER TABLE $targetTable ADD COLUMN IF NOT EXISTS $tmpColumn varchar") *>
        run(s"UPDATE $targetTable SET $tmpColumn = $mergeKeyExpr"),
      // 2. drop the old column
      run(s"ALTER TABLE $targetTable DROP COLUMN IF EXISTS ${MergeKeyField.name}"),
      // 3. create the column under its new name and copy the values from the temporary column
      run(s"ALTER TABLE $targetTable ADD COLUMN IF NOT EXISTS ${MergeKeyField.name} varchar") *>
        run(s"UPDATE $targetTable SET ${MergeKeyField.name} = $tmpColumn"),
      // 4. drop the temporary column
      run(s"ALTER TABLE $targetTable DROP COLUMN IF EXISTS $tmpColumn")
    )

    for
      completed <- sinkPropertyManager.getProperty(tableName, progressProperty).map(_.fold(0)(_.toInt))
      _ <- ZIO.foreachDiscard(steps.zipWithIndex.drop(completed)) { (step, index) =>
        step *> sinkPropertyManager.setProperty(tableName, progressProperty, (index + 1).toString)
      }
      _ <- sinkPropertyManager.removeProperty(tableName, progressProperty)
      _ <- zlog("Successfully migrated %s for %s", MergeKeyField.name, targetTable)
    yield ()
