package com.sneaksanddata.arcane.framework
package services.synapse

import logging.ZIOLogAnnotations.*
import models.app.StreamContext
import models.schemas.DataRow
import models.settings.VersionedDataGraphBuilderSettings
import services.metrics.DeclaredMetrics
import services.streaming.base.StreamDataProvider
import services.synapse.base.SynapseLinkDataProvider

import com.sneaksanddata.arcane.framework.services.synapse.versioning.SynapseWatermark
import zio.metrics.Metric
import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

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
          previousVersion   <- version
          currentVersion    <- dataProvider.getCurrentVersion(previousVersion)
          hasVersionUpdated <- ZIO.succeed(previousVersion.version != currentVersion.version)
          _ <- ZIO.when(hasVersionUpdated) {
            for
              _ <- zlog(
                "Batch version updated from %s to %s, checking for changes",
                previousVersion.version,
                currentVersion.version
              )
              _ <- checkEmpty(previousVersion)
            yield ()
          }
          _ <- ZIO.unless(hasVersionUpdated) {
            for _ <- zlog(
                "No changes, next check in %s seconds, staying at %s version",
                settings.changeCaptureInterval.toSeconds.toString,
                previousVersion.version
              ) *> ZIO.sleep(zio.Duration.fromJava(settings.changeCaptureInterval))
            yield ()
          }
// TODO: re-implement this when watermarking is integrated
//          _ <- ZIO.when(newVersion.isDefined) {
//            for _ <- ZIO.succeed(
//                ChronoUnit.SECONDS
//                  .between(
//                    OffsetDateTime.parse(newVersion.get, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH.mm.ssX")),
//                    OffsetDateTime.now()
//                  )
//                  .toDouble
//              ) @@ batchDelayInterval
//            yield ()
//          }
        yield Some((currentVersion, previousVersion) -> ZIO.succeed(currentVersion))
      }
      .flatMap {
        case (currentVersion, previousVersion) if currentVersion.version > previousVersion.version =>
          dataProvider.requestChanges(previousVersion)
        case _ => ZStream.empty
      }

  // TODO: move to extension method befor implementing watermarking
  private def checkEmpty(previousVersion: SynapseWatermark): Task[Unit] =
    for
      _         <- zlog(s"Received versioned batch: ${previousVersion.version}")
      isChanged <- dataProvider.hasChanges(previousVersion)
      _ <- ZIO.unless(isChanged) {
        zlog(
          s"No data in the batch, next check in ${settings.changeCaptureInterval.toSeconds} seconds"
        ) *> ZIO.sleep(
          settings.changeCaptureInterval
        )
      }
      _ <- ZIO.when(isChanged) {
        zlog(s"Data found in the batch: ${previousVersion.version}, continuing") *> ZIO.unit
      }
    yield ()

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
