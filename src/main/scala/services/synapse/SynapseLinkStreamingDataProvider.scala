package com.sneaksanddata.arcane.framework
package services.synapse

import services.streaming.base.StreamDataProvider
import models.DataRow
import models.app.StreamContext
import models.settings.VersionedDataGraphBuilderSettings
import services.synapse.base.SynapseLinkDataProvider
import logging.ZIOLogAnnotations.*

import zio.{ZIO, ZLayer}
import zio.stream.{ZSink, ZStream}

class SynapseLinkStreamingDataProvider(dataProvider: SynapseLinkDataProvider,
                                       settings: VersionedDataGraphBuilderSettings,
                                       streamContext: StreamContext) extends StreamDataProvider:
  override type StreamElementType = DataRow

  override def stream: ZStream[Any, Throwable, DataRow] = if streamContext.IsBackfilling then
      dataProvider.requestBackfill
  else
    ZStream.unfold(dataProvider.firstVersion)(version =>
      val changes = ZStream.fromZIO(version).flatMap(dataProvider.requestChanges)
      Some(
        changes.map(r => r._1)
        ->
        changes
          .runHead
          .flatMap {
            case Some(newVersion) => ZIO.succeed(newVersion._2)
            case None => for
              oldVersion <- version
              _ <- zlog("No changes, next check in %s seconds, staying at %s timestamp", settings.changeCaptureInterval.toSeconds.toString, oldVersion)
              _ <- ZIO.sleep(zio.Duration.fromJava(settings.changeCaptureInterval))
            yield oldVersion
          }
    )).flatten

object SynapseLinkStreamingDataProvider:

  /**
   * The environment for the MsSqlStreamingDataProvider.
   */
  type Environment = SynapseLinkDataProvider & VersionedDataGraphBuilderSettings & StreamContext

  /**
   * Creates a new instance of the MsSqlStreamingDataProvider class.
   * @param dataProvider Underlying data provider.
   * @return A new instance of the MsSqlStreamingDataProvider class.
   */
  def apply(dataProvider: SynapseLinkDataProvider,
            settings: VersionedDataGraphBuilderSettings,
            streamContext: StreamContext): SynapseLinkStreamingDataProvider =
    new SynapseLinkStreamingDataProvider(dataProvider, settings, streamContext)

  /**
   * The ZLayer that creates the MsSqlStreamingDataProvider.
   */
  val layer: ZLayer[Environment, Nothing, StreamDataProvider] =
    ZLayer {
      for dataProvider <- ZIO.service[SynapseLinkDataProvider]
          settings <- ZIO.service[VersionedDataGraphBuilderSettings]
          streamContext <- ZIO.service[StreamContext]
      yield SynapseLinkStreamingDataProvider(dataProvider, settings, streamContext)
    }
