package com.sneaksanddata.arcane.framework
package services.synapse

import logging.ZIOLogAnnotations.*
import models.app.StreamContext
import models.schemas.DataRow
import models.settings.VersionedDataGraphBuilderSettings
import services.metrics.DeclaredMetrics
import services.streaming.base.StreamDataProvider
import services.synapse.base.SynapseLinkDataProvider

import zio.metrics.Metric
import zio.stream.ZStream
import zio.{ZIO, ZLayer}

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

class SynapseLinkStreamingDataProvider(
    dataProvider: SynapseLinkDataProvider,
    settings: VersionedDataGraphBuilderSettings,
    streamContext: StreamContext,
    metrics: DeclaredMetrics
) extends StreamDataProvider:

  private val batchDelayInterval = metrics.tagMetric(Metric.gauge("arcane.stream.synapse.processing_lag"))

  override def stream: ZStream[Any, Throwable, DataRow] = if streamContext.IsBackfilling then
    dataProvider.requestBackfill
  else
    ZStream
      .unfoldZIO(dataProvider.firstVersion) { version =>
        for
          previousVersion <- version
          newVersion      <- dataProvider.requestChanges(previousVersion).map(_._2).runHead
          _ <- ZIO.when(newVersion.isEmpty) {
            for
              _ <- zlog(
                "No changes, next check in %s seconds, staying at %s timestamp",
                settings.changeCaptureInterval.toSeconds.toString,
                previousVersion
              )
              _ <- ZIO.sleep(zio.Duration.fromJava(settings.changeCaptureInterval))
            yield ()
          }
          _ <- ZIO.when(newVersion.isDefined) {
            for _ <- ZIO.succeed(
                ChronoUnit.SECONDS
                  .between(
                    OffsetDateTime.parse(newVersion.get, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH.mm.ssX")),
                    OffsetDateTime.now()
                  )
                  .toDouble
              ) @@ batchDelayInterval
            yield ()
          }
        // if we keep staying at previousVersion when no changes have been emitted
        // stream will list increasing number of date prefixes for a slow changing entity, thus accumulating iterative read cost in Azure
        // thus, previousVersion is set to be at **most** lookbackVersion, in case there are no changes
        yield Some(
          (newVersion, previousVersion) -> ZIO
            .succeed(newVersion.getOrElse(previousVersion))
            .flatMap(v => dataProvider.firstVersion.map(fv => (v, fv)))
            .map {
              case (nextVersion, lookbackVersion) if lookbackVersion < nextVersion => nextVersion
              case (_, lookbackVersion)                                            => lookbackVersion
            }
        )
      }
      .flatMap {
        case (Some(_), previousVersion) => dataProvider.requestChanges(previousVersion)
        case (None, _)                  => ZStream.empty
      }
      .map(_._1)

object SynapseLinkStreamingDataProvider:

  /** The environment for the MsSqlStreamingDataProvider.
    */
  type Environment = SynapseLinkDataProvider & VersionedDataGraphBuilderSettings & StreamContext & DeclaredMetrics

  /** Creates a new instance of the MsSqlStreamingDataProvider class.
    * @param dataProvider
    *   Underlying data provider.
    * @return
    *   A new instance of the MsSqlStreamingDataProvider class.
    */
  def apply(
      dataProvider: SynapseLinkDataProvider,
      settings: VersionedDataGraphBuilderSettings,
      streamContext: StreamContext,
      metrics: DeclaredMetrics
  ): SynapseLinkStreamingDataProvider =
    new SynapseLinkStreamingDataProvider(dataProvider, settings, streamContext, metrics)

  /** The ZLayer that creates the MsSqlStreamingDataProvider.
    */
  val layer: ZLayer[Environment, Nothing, StreamDataProvider] =
    ZLayer {
      for
        dataProvider  <- ZIO.service[SynapseLinkDataProvider]
        settings      <- ZIO.service[VersionedDataGraphBuilderSettings]
        streamContext <- ZIO.service[StreamContext]
        metrics       <- ZIO.service[DeclaredMetrics]
      yield SynapseLinkStreamingDataProvider(dataProvider, settings, streamContext, metrics)
    }
