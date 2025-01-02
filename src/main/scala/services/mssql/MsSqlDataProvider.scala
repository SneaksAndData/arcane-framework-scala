package com.sneaksanddata.arcane.framework
package services.mssql

import services.mssql.MsSqlConnection.{BackfillBatch, VersionedBatch}
import services.mssql.query.{LazyQueryResult, QueryRunner, ScalarQueryResult}
import services.streaming.base.{BackfillDataProvider, HasVersion, VersionedDataProvider}

import zio.{Task, ZIO, ZLayer}

import java.time.Duration
import scala.util.{Failure, Try}

given HasVersion[VersionedBatch] with
  type VersionType = Option[Long]

  private val partial: PartialFunction[VersionedBatch, Option[Long]] =
    case (queryResult, version: Long) =>
      // If the database response is empty, we can't extract the version from it and return the old version.
      queryResult.read.headOption match
        case None => Some(version)
        case Some(row) =>
          // If the database response is not empty, we can extract the version from any row of the response.
          // Let's take the first row and try to extract the version from it.
          val dataVersion = row.filter(_.name == "ChangeTrackingVersion") match
            // For logging purposes will be used in the future.
            case Nil => Failure(new UnsupportedOperationException("No ChangeTrackingVersion found in row."))
            case version :: _ => Try(version.value.asInstanceOf[Long])
          dataVersion.toOption

  extension (result: VersionedBatch)
    def getLatestVersion: this.VersionType = partial.applyOrElse(result, (_: VersionedBatch) => None)



/**
 * A data provider that reads the changes from the Microsoft SQL Server.
 * @param msSqlConnection The connection to the Microsoft SQL Server.
 */
class MsSqlDataProvider(msSqlConnection: MsSqlConnection) extends VersionedDataProvider[Long, VersionedBatch]
  with BackfillDataProvider:
  
  implicit val dataQueryRunner: QueryRunner[LazyQueryResult.OutputType, LazyQueryResult] = QueryRunner()
  implicit val versionQueryRunner: QueryRunner[Option[Long], ScalarQueryResult[Long]] = QueryRunner()

  override def requestChanges(previousVersion: Option[Long], lookBackInterval: Duration): Task[VersionedBatch] =
    ZIO.fromFuture(_ => msSqlConnection.getChanges(previousVersion, lookBackInterval))
    
  override def requestBackfill: Task[BackfillBatch] = ZIO.fromFuture(_ => msSqlConnection.backfill)

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
