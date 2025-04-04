package com.sneaksanddata.arcane.framework
package services.synapse

import services.streaming.base.StreamDataProvider
import models.DataRow
import models.app.StreamContext
import models.settings.VersionedDataGraphBuilderSettings
import services.synapse.base.SynapseLinkDataProvider
import logging.ZIOLogAnnotations.*

import zio.{ZIO, ZLayer}
import zio.stream.ZStream

class SynapseLinkStreamingDataProvider(dataProvider: SynapseLinkDataProvider,
                                       settings: VersionedDataGraphBuilderSettings,
                                       streamContext: StreamContext) extends StreamDataProvider:
  override type StreamElementType = DataRow

  override def stream: ZStream[Any, Throwable, DataRow] = if streamContext.IsBackfilling then
      dataProvider.requestBackfill
  else
    ZStream.unfold(ZStream.succeed(Option.empty[String]))(version => Some(
      version
        .flatMap(dataProvider.requestChanges).map(r => r._1)
        .orElseIfEmpty(
          ZStream.fromZIO(
            for {
              _ <- zlog("No changes, next check in %s seconds", settings.changeCaptureInterval.toSeconds.toString)
              _ <- ZIO.sleep(zio.Duration.fromJava(settings.changeCaptureInterval))
            } yield ()
          ).flatMap(_ => ZStream.empty)
        ),
      version.flatMap(dataProvider.requestChanges).take(1).map(v => Some(v._2))
        .orElseIfEmpty(
          version.flatMap { versionValue => 
            zlogStream("No rows emitted from latest scan, staying at %s timestamp for next iteration", versionValue.getOrElse("None"))
              .map(_ => versionValue)
          }
        )
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
