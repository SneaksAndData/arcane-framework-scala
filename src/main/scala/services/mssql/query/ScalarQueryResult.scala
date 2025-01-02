package com.sneaksanddata.arcane.framework
package services.mssql.query

import models.{DataCell, DataRow}
import services.mssql.MsSqlConnection.toArcaneType
import services.mssql.base.{QueryResult, ResultSetOwner}
import services.mssql.query.LazyQueryResult

import java.sql.{ResultSet, Statement}
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
 * Callback function that converts a result set to a result.
 * @tparam Result The type of the result.
 */
type ResultConverter[Result] = ResultSet => Option[Result]

/**
 * Implementation of the [[QueryResult]] trait that reads the scalar result of a query.
 *
 * @param statement The statement used to execute the query.
 * @param resultSet The result set of the query.
 */
class ScalarQueryResult[Result](val statement: Statement, resultSet: ResultSet, resultConverter: ResultConverter[Result])
  extends QueryResult[Option[Result]] with ResultSetOwner:

  /**
   * Reads the result of the query.
   *
   * @return The result of the query.
   */
  override def read: this.OutputType =
    resultSet.getMetaData.getColumnCount match
      case 1 =>
        if resultSet.next() then
          resultConverter(resultSet)
        else
          None
      case _ => None

/**
 * Companion object for [[LazyQueryResult]].
 */
object ScalarQueryResult {
  /**
   * Creates a new [[LazyQueryResult]] object.
   *
   * @param statement The statement used to execute the query.
   * @param resultSet The result set of the query.
   * @return The new [[LazyQueryResult]] object.
   */
  def apply[Result](statement: Statement, resultSet: ResultSet, resultConverter: ResultConverter[Result]): ScalarQueryResult[Result] =
    new ScalarQueryResult[Result](statement, resultSet, resultConverter)
}
