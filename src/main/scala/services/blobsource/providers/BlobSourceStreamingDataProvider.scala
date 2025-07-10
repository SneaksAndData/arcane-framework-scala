package com.sneaksanddata.arcane.framework
package services.blobsource.providers

import logging.ZIOLogAnnotations.zlog
import models.app.StreamContext
import models.schemas.DataRow
import models.settings.VersionedDataGraphBuilderSettings
import services.blobsource.readers.BlobSourceReader
import services.streaming.base.{RowProcessor, StreamDataProvider}

import zio.ZIO
import zio.stream.ZStream

class BlobSourceStreamingDataProvider(
    dataProvider: BlobSourceDataProvider,
    settings: VersionedDataGraphBuilderSettings,
    streamContext: StreamContext
) extends StreamDataProvider:

  /** Returns the stream of elements.
    */
  override def stream: ZStream[Any, Throwable, DataRow] = if streamContext.IsBackfilling then {
    // pending https://github.com/SneaksAndData/arcane-framework-scala/issues/181 to avoid asInstanceOf
    dataProvider.requestBackfill.map(_.asInstanceOf[DataRow])
  } else
    ZStream
      .unfoldZIO(dataProvider.firstVersion) { version =>
        for
          previousVersion <- version
          newVersion      <- dataProvider.nextVersion
          _ <- ZIO.when(newVersion == previousVersion) {
            for
              _ <- zlog(
                "No version updates, next check in %s seconds, current version: %s",
                settings.changeCaptureInterval.toSeconds.toString,
                previousVersion.toString
              )
              _ <- ZIO.sleep(zio.Duration.fromJava(settings.changeCaptureInterval))
            yield ()
          }
        yield Some(
          (newVersion, previousVersion) -> ZIO
            .succeed(newVersion)
            .flatMap(v => dataProvider.firstVersion.map(fv => (v, fv)))
            .map {
              case (nextVersion, lookbackVersion) if lookbackVersion < nextVersion => nextVersion
              case (_, lookbackVersion)                                            => lookbackVersion
            }
        )
      }
      .flatMap {
        case (newVersion, previousVersion) if newVersion > previousVersion =>
          dataProvider.requestChanges(previousVersion)
        // handle unversioned case this way, for now
        case (newVersion, previousVersion) if newVersion == 0 && newVersion == previousVersion =>
          dataProvider.requestChanges(previousVersion)
        case _ => ZStream.empty
      }
      .map(_._1.asInstanceOf[DataRow])
