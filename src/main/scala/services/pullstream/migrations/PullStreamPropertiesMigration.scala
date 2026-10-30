package com.sneaksanddata.arcane.framework
package services.pullstream.migrations

import logging.ZIOLogAnnotations.zlog
import migrations.base.Migration
import models.MetadataKeys
import models.app.PluginStreamContext
import models.settings.TableNaming.parts
import services.base.{MergeServiceClient, StreamingSource}
import services.iceberg.base.SinkPropertyManager
import services.pullstream.PullStreamingSource

import zio.ZIO

class PullStreamPropertiesMigration extends Migration:
  val description: String =
    "Ensures push-stream plugin properties (json-pointer-expression, page-size, decode-chunk-size) are set on the sink table"
  val shortName: String = "pull_stream_properties"
  val uniqueId: Long    = 62849103L

  def execute(
      sinkPropertyManager: SinkPropertyManager,
      context: PluginStreamContext,
      mergeClient: MergeServiceClient,
      streamingSource: StreamingSource
  ): ZIO[Any, Throwable, Unit] =
    val targetTableName = context.sink.targetTableFullName.parts.name
    val jsonPointerKey  = PullStreamingSource.jsonPointerPropertyKey
    val pageSizeKey     = PullStreamingSource.pageSizePropertyKey
    val decodeChunkKey  = PullStreamingSource.decodeChunkSizePropertyKey

    for
      legacyPointer   <- sinkPropertyManager.getProperty(targetTableName, PullStreamingSource.jsonPointerPropertyName)
      existingPointer <- sinkPropertyManager.getProperty(targetTableName, jsonPointerKey)
      _ <- ZIO.when(existingPointer.isEmpty) {
        val pointerValue = legacyPointer.getOrElse("")
        for
          _ <- sinkPropertyManager.setProperty(targetTableName, jsonPointerKey, pointerValue)
          _ <- zlog("Set property %s='%s' on %s", jsonPointerKey, pointerValue, targetTableName)
        yield ()
      }
      _ <- ZIO.when(legacyPointer.isDefined) {
        for
          _ <- sinkPropertyManager.removeProperty(targetTableName, PullStreamingSource.jsonPointerPropertyName)
          _ <- zlog(
            "Removed legacy property %s from %s",
            PullStreamingSource.jsonPointerPropertyName,
            targetTableName
          )
        yield ()
      }

      existingPageSize <- sinkPropertyManager.getProperty(targetTableName, pageSizeKey)
      _ <- ZIO.when(existingPageSize.isEmpty) {
        val pageSizeValue = "1000"
        for
          _ <- sinkPropertyManager.setProperty(targetTableName, pageSizeKey, pageSizeValue)
          _ <- zlog("Set property %s=%s on %s", pageSizeKey, pageSizeValue, targetTableName)
        yield ()
      }

      existingDecodeChunk <- sinkPropertyManager.getProperty(targetTableName, decodeChunkKey)
      _ <- ZIO.when(existingDecodeChunk.isEmpty) {
        val decodeChunkValue = "32"
        for
          _ <- sinkPropertyManager.setProperty(targetTableName, decodeChunkKey, decodeChunkValue)
          _ <- zlog("Set property %s=%s on %s", decodeChunkKey, decodeChunkValue, targetTableName)
        yield ()
      }
    yield ()
