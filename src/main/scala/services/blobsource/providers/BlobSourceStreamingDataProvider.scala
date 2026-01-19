package com.sneaksanddata.arcane.framework
package services.blobsource.providers

import logging.ZIOLogAnnotations.zlog
import models.app.StreamContext
import models.schemas.DataRow
import models.settings.VersionedDataGraphBuilderSettings
import services.blobsource.readers.BlobSourceReader
import services.streaming.base.{RowProcessor, StreamDataProvider}

import com.sneaksanddata.arcane.framework.services.blobsource.BlobSourceVersion
import zio.{Task, ZIO, ZLayer}
import zio.stream.ZStream

class BlobSourceStreamingDataProvider(
    dataProvider: BlobSourceDataProvider,
    settings: VersionedDataGraphBuilderSettings,
    streamContext: StreamContext
) extends StreamDataProvider:

  /** Determines the next version to start emitting elements from, depending on the current and new value available from
    * the data provider
    * @param version
    *   a ZIO task that emits the version used for the last iteration
    * @return
    */
  private def nextVersion(version: Task[BlobSourceVersion]) = for
    previousVersion <- version
    newVersion      <- dataProvider.getCurrentVersion(previousVersion)
    hasChanged <- ZIO.succeed(newVersion != previousVersion)
    _ <- ZIO.when(hasChanged) {
      for
        _ <- zlog(
          "Version has updated to %s",
          newVersion.toString
        )
        _ <- dataProvider.hasChanges(previousVersion) // no-op here, this code will be integrated into generic provider interface
      yield ()
    }
    _ <- ZIO.unless(hasChanged) {
      for
        _ <- zlog(
          "No version updates, next check in %s seconds, current version: %s",
          settings.changeCaptureInterval.toSeconds.toString,
          previousVersion.toString
        )
        _ <- ZIO.sleep(zio.Duration.fromJava(settings.changeCaptureInterval))
      yield ()
    }
  yield Some((newVersion, previousVersion) -> ZIO.succeed(newVersion))

  /** Returns the stream of elements.
    */
  override def stream: ZStream[Any, Throwable, DataRow] = if streamContext.IsBackfilling then {
    // pending https://github.com/SneaksAndData/arcane-framework-scala/issues/181 to avoid asInstanceOf
    dataProvider.requestBackfill.map(_.asInstanceOf[DataRow])
  } else
    ZStream
      .unfoldZIO(dataProvider.firstVersion)(nextVersion)
      .flatMap {
        case (newVersion, previousVersion) if newVersion.versionNumber > previousVersion.versionNumber =>
          dataProvider.requestChanges(previousVersion)
        case _ => ZStream.empty
      }
      .map(_.asInstanceOf[DataRow])

object BlobSourceStreamingDataProvider:
  private type Environment = BlobSourceDataProvider & VersionedDataGraphBuilderSettings & StreamContext

  def apply(
      dataProvider: BlobSourceDataProvider,
      settings: VersionedDataGraphBuilderSettings,
      streamContext: StreamContext
  ): BlobSourceStreamingDataProvider = new BlobSourceStreamingDataProvider(dataProvider, settings, streamContext)

  val layer: ZLayer[Environment, Nothing, StreamDataProvider] =
    ZLayer {
      for
        dataProvider  <- ZIO.service[BlobSourceDataProvider]
        settings      <- ZIO.service[VersionedDataGraphBuilderSettings]
        streamContext <- ZIO.service[StreamContext]
      yield BlobSourceStreamingDataProvider(dataProvider, settings, streamContext)
    }
