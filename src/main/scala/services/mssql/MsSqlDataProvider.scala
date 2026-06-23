package com.sneaksanddata.arcane.framework
package services.mssql

import models.app.PluginStreamContext
import models.settings.sink.SinkSettings
import models.settings.sources.SourceBufferingSettings
import services.iceberg.base.SinkPropertyManager
import services.metrics.DeclaredMetrics
import services.mssql.base.MsSqlStreamingSource
import services.mssql.versioning.MsSqlWatermark
import services.mssql.versioning.MsSqlWatermark.*
import services.streaming.base.{DefaultSourceDataProvider, StructuredZStream}
import services.streaming.throughput.base.ThroughputShaperBuilder

import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

/** A data provider that reads the changes from the Microsoft SQL Server.
  */
class MsSqlDataProvider(
    streamingSource: MsSqlStreamingSource,
    sinkPropertyManager: SinkPropertyManager,
    sinkSettings: SinkSettings,
    throughputShaperBuilder: ThroughputShaperBuilder,
    sourceBufferingSettings: SourceBufferingSettings,
    declaredMetrics: DeclaredMetrics
) extends DefaultSourceDataProvider[MsSqlWatermark](
      streamingSource,
      sinkPropertyManager,
      sinkSettings,
      throughputShaperBuilder,
      sourceBufferingSettings,
      declaredMetrics
    ):

  def hasChanges(previousVersion: MsSqlWatermark): Task[Boolean] = streamingSource.hasChanges(previousVersion)

  def getCurrentVersion(previousVersion: MsSqlWatermark): Task[MsSqlWatermark] =
    for
      // get current version from CHANGE_TRACKING_CURRENT_VERSION() and the commit time associated with it
      version <- streamingSource
        .getVersion(QueryProvider.getCurrentVersionQuery)
        .flatMap {
          case Some(value) =>
            for commitTime <- streamingSource.getVersionCommitTime(value)
            yield MsSqlWatermark.fromChangeTrackingVersion(value, commitTime)
          case None => ZIO.succeed(previousVersion)
        }
    yield version

  override protected def changeStream(previousVersion: MsSqlWatermark): ZStream[Any, Throwable, StructuredZStream] =
    streamingSource.getChanges(previousVersion)

/** The companion object for the MsSqlDataProvider class.
  */
object MsSqlDataProvider:

  /** The ZLayer that creates the MsSqlDataProvider.
    */
  val layer =
    ZLayer {
      for
        context         <- ZIO.service[PluginStreamContext]
        reader          <- ZIO.service[MsSqlStreamingSource]
        propertyManager <- ZIO.service[SinkPropertyManager]
        shaperBuilder   <- ZIO.service[ThroughputShaperBuilder]
        metrics         <- ZIO.service[DeclaredMetrics]
      yield new MsSqlDataProvider(
        reader,
        propertyManager,
        context.sink,
        shaperBuilder,
        context.source.buffering,
        metrics
      )
    }
