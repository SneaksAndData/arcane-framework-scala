package com.sneaksanddata.arcane.framework
package services.mssql.query

import models.schemas.{DataCell, DataRow}
import services.mssql.base.QueryResult
import services.mssql.{SqlDataCell, SqlDataRow, given_Conversion_SqlDataRow_DataRow}
import services.mssql.query.MsSqlResultSet.*
import utils.SqlUtils.toArcaneType

import zio.ZIO
import zio.stream.ZStream

import java.sql.{ResultSet, Statement}
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/** Lazy-list based implementation of [[QueryResult]].
  *
  * @param resultSet
  *   The result set of the query.
  */
class ResultSetZIO(protected val resultSet: ResultSet)
    extends QueryResult[ZStream[Any, Throwable, DataRow]]:

  /** Reads the result of the query.
    *
    * @return
    *   The result of the query.
    */
  override def read: this.OutputType = resultSet.toZStream.map(implicitly)


/** Companion object for [[ResultSetZIO]].
  */
object ResultSetZIO:

  /** The output type of the query result.
    */
  type OutputType = ZStream[Any, Throwable, DataRow]

  /** Creates a new [[ResultSetZIO]] object.
   *
   * @param resultSet
    *   The result set of the query.
    * @return
    * The new [[ResultSetZIO]] object.
    */
  def apply(resultSet: ResultSet): ResultSetZIO =
    new ResultSetZIO(resultSet)
