package com.sneaksanddata.arcane.framework
package services.mssql

import logging.ZIOLogAnnotations.zlog
import models.app.StreamContext
import models.schemas.{ArcaneType, DataCell, DataRow}
import models.settings.VersionedDataGraphBuilderSettings
import services.mssql.MsSqlConnection.{DataBatch, VersionedBatch}
import services.mssql.base.QueryResult
import services.streaming.base.StreamDataProvider

import zio.stream.ZStream
import zio.{ZIO, ZLayer}

import java.nio.ByteBuffer
import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}

/** Streaming data provider for Microsoft SQL Server.
  */
class MsSqlStreamingDataProvider(
    dataProvider: MsSqlDataProvider,
    settings: VersionedDataGraphBuilderSettings,
    streamContext: StreamContext
) extends StreamDataProvider:

  type StreamElementType = DataRow

  /** @inheritdoc
    */
  override def stream: ZStream[Any, Throwable, DataRow] =
    val stream =
      if streamContext.IsBackfilling then dataProvider.requestBackfill
      else ZStream.unfoldZIO(None)(v => continueStream(v)).flatMap(readDataBatch)
    stream
      .map(row =>
        row.map {
          case DataCell(name, ArcaneType.TimestampType, value) if value != null =>
            DataCell(
              name,
              ArcaneType.TimestampType,
              LocalDateTime.ofInstant(value.asInstanceOf[Timestamp].toInstant, ZoneOffset.UTC)
            )

          case DataCell(name, ArcaneType.TimestampType, value) if value == null =>
            DataCell(name, ArcaneType.TimestampType, null)

          case DataCell(name, ArcaneType.ByteArrayType, value) if value != null =>
            DataCell(name, ArcaneType.ByteArrayType, ByteBuffer.wrap(value.asInstanceOf[Array[Byte]]))

          case DataCell(name, ArcaneType.ByteArrayType, value) if value == null =>
            DataCell(name, ArcaneType.ByteArrayType, null)

          case DataCell(name, ArcaneType.ShortType, value) =>
            DataCell(name, ArcaneType.IntType, value.asInstanceOf[Short].toInt)

          case other => other
        }
      )

  private def readDataBatch[T <: AutoCloseable & QueryResult[LazyList[DataRow]]](
      batch: T
  ): ZStream[Any, Throwable, DataRow] =
    for
      data     <- ZStream.acquireReleaseWith(ZIO.succeed(batch))(b => ZIO.succeed(b.close()))
      rowsList <- ZStream.fromZIO(ZIO.attemptBlocking(data.read))
      row      <- ZStream.fromIterable(rowsList)
    yield row

  private def continueStream(previousVersion: Option[Long]): ZIO[Any, Throwable, Some[(DataBatch, Option[Long])]] =
    for
      versionedBatch <- dataProvider.requestChanges(previousVersion, settings.lookBackInterval)
      _              <- zlog(s"Received versioned batch: ${versionedBatch.getLatestVersion}")
      _              <- maybeSleep(versionedBatch)
      latestVersion    = versionedBatch.getLatestVersion
      (queryResult, _) = versionedBatch
      _ <- zlog(s"Latest version: ${versionedBatch.getLatestVersion}")
    yield Some(queryResult, latestVersion)

  private def maybeSleep(versionedBatch: VersionedBatch): ZIO[Any, Nothing, Unit] =
    versionedBatch match
      case (queryResult, _) =>
        val headOption = queryResult.read.headOption
        if headOption.isEmpty then
          zlog("No data in the batch, sleeping for the configured interval.") *> ZIO.sleep(
            settings.changeCaptureInterval
          )
        else zlog("Data found in the batch, continuing without sleep.") *> ZIO.unit

object MsSqlStreamingDataProvider:

  /** The environment for the MsSqlStreamingDataProvider.
    */
  type Environment = MsSqlDataProvider & VersionedDataGraphBuilderSettings & StreamContext

  /** Creates a new instance of the MsSqlStreamingDataProvider class.
    * @param dataProvider
    *   Underlying data provider.
    * @param settings
    *   The settings for the data graph builder.
    * @return
    *   A new instance of the MsSqlStreamingDataProvider class.
    */
  def apply(
      dataProvider: MsSqlDataProvider,
      settings: VersionedDataGraphBuilderSettings,
      streamContext: StreamContext
  ): MsSqlStreamingDataProvider =
    new MsSqlStreamingDataProvider(dataProvider, settings, streamContext)

  /** The ZLayer that creates the MsSqlStreamingDataProvider.
    */
  val layer: ZLayer[Environment, Nothing, StreamDataProvider] =
    ZLayer {
      for
        dataProvider  <- ZIO.service[MsSqlDataProvider]
        settings      <- ZIO.service[VersionedDataGraphBuilderSettings]
        streamContext <- ZIO.service[StreamContext]
      yield MsSqlStreamingDataProvider(dataProvider, settings, streamContext)
    }
