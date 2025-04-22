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
  
  override def stream: ZStream[Any, Throwable, DataRow] = if streamContext.IsBackfilling then
      dataProvider.requestBackfill
  else
    ZStream.unfoldZIO(dataProvider.firstVersion) { version => for
        previousVersion <- version
        newVersion <- dataProvider.requestChanges(previousVersion).map(_._2).runHead
        _ <- ZIO.when(newVersion.isEmpty) { for
          _ <- zlog("No changes, next check in %s seconds, staying at %s timestamp", settings.changeCaptureInterval.toSeconds.toString, previousVersion)
          _ <- ZIO.sleep(zio.Duration.fromJava(settings.changeCaptureInterval))
         yield ()
        }
      // if we keep staying at previousVersion when no changes have been emitted
      // stream will list increasing number of date prefixes for a slow changing entity, thus accumulating iterative read cost in Azure
      // thus, previousVersion is set to be at **most** lookbackVersion, in case there are no changes
      yield Some((newVersion, previousVersion) -> ZIO.succeed(newVersion.getOrElse(previousVersion)).flatMap(v => dataProvider.firstVersion.map(fv => (v, fv))).map {
        case (nextVersion, lookbackVersion) if lookbackVersion < nextVersion => nextVersion
        case (_, lookbackVersion) => lookbackVersion
      })
    }.flatMap {
      case (Some(_), previousVersion) => dataProvider.requestChanges(previousVersion)
      case (None, _) => ZStream.empty
    }.map(_._1)

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
