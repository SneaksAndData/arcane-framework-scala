package com.sneaksanddata.arcane.framework
package services.mssql

import models.schemas.DataRow
import models.settings.{BackfillSettings, VersionedDataGraphBuilderSettings}
import services.mssql.base.MsSqlReader
import services.streaming.base.{BackfillDataProvider, VersionedDataProvider}

import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

import java.time.{Instant, OffsetDateTime, ZoneOffset}

/** A data provider that reads the changes from the Microsoft SQL Server.
  * @param reader
  *   The connection to the Microsoft SQL Server.
  */
class MsSqlDataProvider(
    reader: MsSqlReader,
    settings: VersionedDataGraphBuilderSettings,
    backfillSettings: BackfillSettings
) extends VersionedDataProvider[MsSqlChangeVersion, DataRow]
    with BackfillDataProvider[DataRow]:

  override def requestChanges(previousVersion: MsSqlChangeVersion): ZStream[Any, Throwable, DataRow] =
    reader.getChanges(previousVersion)

  def hasChanges(previousVersion: MsSqlChangeVersion): Task[Boolean] = reader.hasChanges(previousVersion)

  def getCurrentVersion(previousVersion: MsSqlChangeVersion): Task[MsSqlChangeVersion] =
    for
      currentTime <- ZIO.succeed(OffsetDateTime.now(ZoneOffset.UTC))
      version     <- reader.getVersion(QueryProvider.getChangeTrackingVersionQuery(currentTime, reader.formatter))
    yield MsSqlChangeVersion(versionNumber = version, waterMarkTime = currentTime)

  /** The first version of the data.
    */
  override def firstVersion: Task[MsSqlChangeVersion] =
    for
      lookBackTime <- ZIO.succeed(
        OffsetDateTime.ofInstant(Instant.now().minusSeconds(settings.lookBackInterval.toSeconds), ZoneOffset.UTC)
      )
      version <- reader.getVersion(QueryProvider.getChangeTrackingVersionQuery(lookBackTime, reader.formatter))
    yield MsSqlChangeVersion(versionNumber = version, waterMarkTime = lookBackTime)

  /** Provides the backfill data.
    *
    * @return
    *   A task that represents the backfill data.
    */
  override def requestBackfill: ZStream[Any, Throwable, DataRow] = reader.backfill

/** The companion object for the MsSqlDataProvider class.
  */
object MsSqlDataProvider:

  /** The ZLayer that creates the MsSqlDataProvider.
    */
  val layer =
    ZLayer {
      for
        connection        <- ZIO.service[MsSqlReader]
        versionedSettings <- ZIO.service[VersionedDataGraphBuilderSettings]
        backfillSettings  <- ZIO.service[BackfillSettings]
      yield new MsSqlDataProvider(connection, versionedSettings, backfillSettings)
    }
