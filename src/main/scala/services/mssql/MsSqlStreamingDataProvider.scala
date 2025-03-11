package com.sneaksanddata.arcane.framework
package services.mssql

import logging.ZIOLogAnnotations.zlog
import models.DataRow
import models.settings.VersionedDataGraphBuilderSettings
import services.mssql.MsSqlConnection.{DataBatch, VersionedBatch}
import services.mssql.base.MssqlVersionedDataProvider
import services.streaming.base.StreamDataProvider

import zio.ZIO
import zio.stream.ZStream

class MsSqlStreamingDataProvider(dataProvider: MssqlVersionedDataProvider[Long, VersionedBatch],
                                 settings: VersionedDataGraphBuilderSettings) extends StreamDataProvider:

  type StreamElementType = DataRow

  override def stream: ZStream[Any, Throwable, DataRow] =
    for data <- ZStream.unfoldZIO(None)(v => continueStream(v))
        aquired <- ZStream.acquireReleaseWith(ZIO.succeed(data))(b => ZIO.succeed(b.close()))
        rowsList <- ZStream.fromZIO(ZIO.attemptBlocking(data.read))
        row <- ZStream.fromIterable(rowsList)
    yield row

  private def continueStream(previousVersion: Option[Long]): ZIO[Any, Throwable, Some[(DataBatch, Option[Long])]] =
    for versionedBatch <- dataProvider.requestChanges(previousVersion, settings.lookBackInterval)
      _ <- zlog(s"Received versioned batch: ${versionedBatch.getLatestVersion}")
      latestVersion = versionedBatch.getLatestVersion
      (queryResult, _) = versionedBatch
      _ <- zlog(s"Latest version: ${versionedBatch.getLatestVersion}")
    yield Some(queryResult, latestVersion)
