package com.sneaksanddata.arcane.framework
package migrations.m1

import logging.ZIOLogAnnotations.zlog
import migrations.base.Migration
import models.app.{PluginStreamContext, StreamDocumentation}
import models.settings.TableNaming.parts
import services.base.{MergeServiceClient, StreamingSource}
import services.iceberg.base.SinkPropertyManager

import zio.ZIO

class TableCommentMigration extends Migration:
  val description: String =
    "Ensures sink table comment property is set to 'Table managed by {plugin_name} {github_repo_link}'"
  val shortName: String = "table_comment"
  val uniqueId: Long    = 59382104L

  def execute(
      sinkPropertyManager: SinkPropertyManager,
      context: PluginStreamContext,
      mergeClient: MergeServiceClient,
      streamingSource: StreamingSource
  ): ZIO[Any, Throwable, Unit] =
    val targetTableName = context.sink.targetTableFullName.parts.name

    for
      documentation <- StreamDocumentation.fromEnvironment
      commentValue = documentation.asComment
      existingComment <- sinkPropertyManager.getProperty(targetTableName, "comment")
      _ <- ZIO.when(!existingComment.contains(commentValue)) {
        for
          _ <- zlog("Updating comment property on %s to '%s'", targetTableName, commentValue)
          _ <- sinkPropertyManager.comment(targetTableName, commentValue)
          _ <- zlog("Successfully updated comment property on %s", targetTableName)
        yield ()
      }
    yield ()
