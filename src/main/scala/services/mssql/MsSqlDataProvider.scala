package com.sneaksanddata.arcane.framework
package services.mssql

import models.schemas.DataRow
import services.mssql.{MsSqlBatch, MsSqlVersionedBatch}
import services.mssql.base.{MsSqlReader, MssqlVersionedDataProvider}
import services.streaming.base.{BackfillDataProvider, HasVersion, VersionedDataProvider}

import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

import java.time.Duration
import scala.util.{Failure, Try}

private def readDataBatch[T <: AutoCloseable & QueryResult[Iterator[DataRow]]](
                                                                                batch: T
                                                                              ): ZStream[Any, Throwable, DataRow] =
  for
    data <- ZStream.acquireReleaseWith(ZIO.succeed(batch))(b => ZIO.succeed(b.close()))
    rowsList <- ZStream.fromZIO(ZIO.attemptBlocking(data.read))
    row <- ZStream.fromIterator(rowsList, 1)
  yield row

given HasVersion[MsSqlVersionedBatch] with
  type VersionType = Option[Long]

  private val partial: PartialFunction[MsSqlVersionedBatch, Option[Long]] =
    case (queryResult, version: Long) =>
      // If the database response is empty, we can't extract the version from it and return the old version.
      queryResult.read.nextOption() match
        case None      => Some(version)
        case Some(row) =>
          // If the database response is not empty, we can extract the version from any row of the response.
          // Let's take the first row and try to extract the version from it.
          val dataVersion = row.filter(_.name == "ChangeTrackingVersion") match
            // For logging purposes will be used in the future.
            case Nil          => Failure(new UnsupportedOperationException("No ChangeTrackingVersion found in row."))
            case version :: _ => Try(version.value.asInstanceOf[Long])
          dataVersion.toOption

  extension (result: MsSqlVersionedBatch)
    def getLatestVersion: this.VersionType = partial.applyOrElse(result, (_: MsSqlVersionedBatch) => None)

/** A data provider that reads the changes from the Microsoft SQL Server.
  * @param reader
  *   The connection to the Microsoft SQL Server.
  */
class MsSqlDataProvider(reader: MsSqlReader)
    extends VersionedDataProvider[Long, MsSqlVersionedBatch]
    with BackfillDataProvider[DataRow]:

//  override def requestChanges(previousVersion: Option[Long]): Task[MsSqlVersionedBatch] =
//    msSqlConnection.getChanges(previousVersion, lookBackInterval)

  //override def requestBackfill: ZStream[Any, Throwable, DataRow] = msSqlConnection.backfill

  override def requestChanges(previousVersion: Long): ZStream[Any, Throwable, MsSqlVersionedBatch] = reader.getChanges(previousVersion)

  /** The first version of the data.
   */
  override def firstVersion: Task[Long] = ???

  /** Provides the backfill data.
   *
   * @return
   * A task that represents the backfill data.
   */
  override def requestBackfill: ZStream[Any, Throwable, DataRow] = reader.backfill

/** The companion object for the MsSqlDataProvider class.
  */
object MsSqlDataProvider:

  /** The ZLayer that creates the MsSqlDataProvider.
    */
  val layer: ZLayer[MsSqlReader, Nothing, MsSqlDataProvider] =
    ZLayer {
      for connection <- ZIO.service[MsSqlReader] yield new MsSqlDataProvider(connection)
    }
