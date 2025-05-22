package com.sneaksanddata.arcane.framework
package services.mssql.query

import services.mssql.base.QueryResult

import zio.ZIO

import java.sql.{ResultSet, Statement}

/** Callback function that converts a result set to a result.
  * @tparam Result
  *   The type of the result.
  */
type ResultConverter[Result] = ResultSet => Option[Result]

/** Implementation of the [[QueryResult]] trait that reads the scalar result of a query.
  * @param resultSet
  *   The result set of the query.
  */
class ScalarQueryResultZIO[Result](
    protected val resultSet: ResultSet,
    resultConverter: ResultConverter[Result]
) extends QueryResult[ZIO[zio.Scope, Throwable, Option[Result]]]:

  /** Reads the result of the query.
    *
    * @return
    *   The result of the query.
    */
  override def read: this.OutputType = ZIO.attemptBlocking(resultSet.getMetaData.getColumnCount match
    case 1 =>
      if resultSet.next() then resultConverter(resultSet)
      else None
    case _ => None)
    

/** Companion object for [[ScalarQueryResultZIO]].
  */
object ScalarQueryResultZIO:

  /** Creates a new [[ScalarQueryResultZIO]] object.
    * @param resultSet
    *   The result set of the query.
    * @return
    * The new [[ScalarQueryResultZIO]] object.
    */
  def apply[Result](
      resultSet: ResultSet,
      resultConverter: ResultConverter[Result]
  ): ScalarQueryResultZIO[Result] =
    new ScalarQueryResultZIO[Result](resultSet, resultConverter)
