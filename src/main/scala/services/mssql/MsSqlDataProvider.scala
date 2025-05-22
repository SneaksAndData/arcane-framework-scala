package com.sneaksanddata.arcane.framework
package services.mssql

import models.schemas.DataRow
import services.mssql.MsSqlConnection.VersionedBatch
import services.mssql.base.MssqlVersionedDataProvider
import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlog
import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

import java.time.Duration
import scala.util.{Failure, Try}


extension (result: VersionedBatch)
  def getLatestVersion: ZIO[Any, Throwable, Option[Long]] = result match
    case (queryResult, version: Long) =>
      queryResult.read.runHead.flatMap {
        // If the database response is empty, we can't extract the version from it and return the old version.
        case None => ZIO.succeed(Some(version))
        case Some(row) =>
          // If the database response is not empty, we can extract the version from any row of the response.
          // Let's take the first row and try to extract the version from it.
          val dataVersion = row.filter(_.name == "ChangeTrackingVersion") match
            // Hard fail when ChangeTrackingVersion is not found
            case Nil => ZIO.die(new Throwable("No ChangeTrackingVersion found in row."))
            case version :: _ => ZIO.attempt(version.value.asInstanceOf[Long]).map(Option.apply).orElse(ZIO.succeed(Option.empty[Long]))
          dataVersion
      }

/** A data provider that reads the changes from the Microsoft SQL Server.
  * @param msSqlConnection
  *   The connection to the Microsoft SQL Server.
  */
class MsSqlDataProvider(msSqlConnection: MsSqlConnection)
    extends MssqlVersionedDataProvider[Long, VersionedBatch]
    with MssqlBackfillDataProvider:

  override def requestChanges(previousVersion: Option[Long], lookBackInterval: Duration): Task[VersionedBatch] =
    msSqlConnection.getChanges(previousVersion, lookBackInterval)

  override def requestBackfill: ZStream[Any, Throwable, DataRow] = msSqlConnection.backfill

/** The companion object for the MsSqlDataProvider class.
  */
object MsSqlDataProvider:

  /** The ZLayer that creates the MsSqlDataProvider.
    */
  val layer: ZLayer[MsSqlConnection, Nothing, MsSqlDataProvider] =
    ZLayer {
      for connection <- ZIO.service[MsSqlConnection] yield new MsSqlDataProvider(connection)
    }
