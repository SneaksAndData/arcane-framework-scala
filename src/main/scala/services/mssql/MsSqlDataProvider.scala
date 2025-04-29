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
