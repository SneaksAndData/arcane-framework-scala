package com.sneaksanddata.arcane.framework
package services.mssql

import logging.ZIOLogAnnotations.zlog
import models.DataRow
import models.settings.VersionedDataGraphBuilderSettings
import services.mssql.MsSqlConnection.{DataBatch, VersionedBatch}
import services.mssql.base.MssqlVersionedDataProvider
import services.streaming.base.StreamDataProvider

import zio.{ZIO, ZLayer}
import zio.stream.ZStream

/**
 * Streaming data provider for Microsoft SQL Server.
 */
class MsSqlStreamingDataProvider(dataProvider: MssqlVersionedDataProvider[Long, VersionedBatch],
                                 settings: VersionedDataGraphBuilderSettings) extends StreamDataProvider:

  type StreamElementType = DataRow

  /**
   * @inheritdoc
   */
  override def stream: ZStream[Any, Throwable, DataRow] =
    for data <- ZStream.unfoldZIO(None)(v => continueStream(v))
        aquired <- ZStream.acquireReleaseWith(ZIO.succeed(data))(b => ZIO.succeed(b.close()))
        rowsList <- ZStream.fromZIO(ZIO.attemptBlocking(data.read))
        row <- ZStream.fromIterable(rowsList)
    yield row

  private def continueStream(previousVersion: Option[Long]): ZIO[Any, Throwable, Some[(DataBatch, Option[Long])]] =
    for _ <- maybeSleep(previousVersion)
      versionedBatch <- dataProvider.requestChanges(previousVersion, settings.lookBackInterval)
      _ <- zlog(s"Received versioned batch: ${versionedBatch.getLatestVersion}")
      latestVersion = versionedBatch.getLatestVersion
      (queryResult, _) = versionedBatch
      _ <- zlog(s"Latest version: ${versionedBatch.getLatestVersion}")
    yield Some(queryResult, latestVersion)

  private def maybeSleep(previousVersion: Option[Long]): ZIO[Any, Nothing, Unit] =
    previousVersion match
      case Some(_) => ZIO.sleep(settings.changeCaptureInterval)
      case None => ZIO.unit

object MsSqlStreamingDataProvider:

  /**
   * The environment for the MsSqlStreamingDataProvider.
   */
  type Environment = MssqlVersionedDataProvider[Long, VersionedBatch]
    & VersionedDataGraphBuilderSettings


  /**
   * Creates a new instance of the MsSqlStreamingDataProvider class.
   * @param dataProvider Underlying data provider.
   * @param settings    The settings for the data graph builder.
   * @return A new instance of the MsSqlStreamingDataProvider class.
   */
  def apply(dataProvider: MssqlVersionedDataProvider[Long, VersionedBatch],
            settings: VersionedDataGraphBuilderSettings): MsSqlStreamingDataProvider =
    new MsSqlStreamingDataProvider(dataProvider, settings)

  /**
   * The ZLayer that creates the MsSqlStreamingDataProvider.
   */
  val layer: ZLayer[Environment, Nothing, StreamDataProvider] =
    ZLayer {
      for dataProvider <- ZIO.service[MssqlVersionedDataProvider[Long, VersionedBatch]]
          settings <- ZIO.service[VersionedDataGraphBuilderSettings]
      yield MsSqlStreamingDataProvider(dataProvider, settings)
    }
