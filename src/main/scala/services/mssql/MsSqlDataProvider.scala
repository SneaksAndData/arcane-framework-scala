package com.sneaksanddata.arcane.framework
package services.mssql

import services.mssql.MsSqlConnection.{BackfillBatch, VersionedBatch}
import services.mssql.base.MssqlVersionedDataProvider
import services.streaming.base.HasVersion

import com.sneaksanddata.arcane.framework.models.DataRow
import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

import java.time.Duration
import scala.util.{Failure, Try}
//
//given HasVersion[VersionedBatch] with
//  type VersionType = Option[Long]
//
//  private val partial: PartialFunction[VersionedBatch, Option[Long]] =
//    case (queryResult, version: Long) =>
//      // If the database response is empty, we can't extract the version from it and return the old version.
//      queryResult.headOption match
//        case None => Some(version)
//        case Some(row) =>
//          // If the database response is not empty, we can extract the version from any row of the response.
//          // Let's take the first row and try to extract the version from it.
//          val dataVersion = row.filter(_.name == "ChangeTrackingVersion") match
//            // For logging purposes will be used in the future.
//            case Nil => Failure(new UnsupportedOperationException("No ChangeTrackingVersion found in row."))
//            case version :: _ => Try(version.value.asInstanceOf[Long])
//          dataVersion.toOption
//
//  extension (result: VersionedBatch)
//    def getLatestVersion: this.VersionType = partial.applyOrElse(result, (_: VersionedBatch) => None)



/**
 * A data provider that reads the changes from the Microsoft SQL Server.
 * @param msSqlConnection The connection to the Microsoft SQL Server.
 */
class MsSqlDataProvider(msSqlConnection: MsSqlConnection) extends MssqlVersionedDataProvider[Long, VersionedBatch]
  with MssqlBackfillDataProvider:
  
  override def requestChanges(previousVersion: Option[Long], lookBackInterval: Duration): Task[VersionedBatch] =
    msSqlConnection.getChanges(previousVersion, lookBackInterval)
    
  override def requestBackfill:  ZStream[Any, Throwable, DataRow] = msSqlConnection.backfill

/**
 * The companion object for the MsSqlDataProvider class.
 */
object MsSqlDataProvider:

  /**
   * The ZLayer that creates the MsSqlDataProvider.
   */
  val layer: ZLayer[MsSqlConnection, Nothing, MsSqlDataProvider] =
    ZLayer {
      for connection <- ZIO.service[MsSqlConnection] yield new MsSqlDataProvider(connection)
    }
