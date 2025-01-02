package com.sneaksanddata.arcane.framework
package services.mssql.query

import models.{DataCell, DataRow}
import services.mssql.MsSqlConnection.toArcaneType
import services.mssql.base.{CanPeekHead, QueryResult, ResultSetOwner}

import java.sql.{ResultSet, Statement}
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}


/**
 * Lazy-list based implementation of [[QueryResult]].
 *
 * @param statement The statement used to execute the query.
 * @param resultSet The result set of the query.
 */
class LazyQueryResult(protected val statement: Statement, resultSet: ResultSet, eagerHead: List[DataRow]) extends QueryResult[LazyList[DataRow]]
   with CanPeekHead[LazyList[DataRow]] with ResultSetOwner:

  /**
   * Reads the result of the query.
   *
   * @return The result of the query.
   */
  override def read: this.OutputType =
    val columns = resultSet.getMetaData.getColumnCount
    eagerHead.to(LazyList) #::: LazyList.continually(resultSet)
      .takeWhile(_.next())
      .map(row => {
        toDataRow(row, columns, List.empty) match {
          case Success(dataRow) => dataRow
          case Failure(exception) => throw exception
        }
      })

  /**
   * Peeks the head of the result of the SQL query mapped to an output type.
   *
   * @return The head of the result of the query.
   */
  def peekHead: QueryResult[this.OutputType] & CanPeekHead[this.OutputType] =
    new LazyQueryResult(statement, resultSet, read.headOption.toList)

  @tailrec
  private def toDataRow(row: ResultSet, columns: Int, acc: DataRow): Try[DataRow] =
    if columns == 0 then Success(acc)
    else
      val name = row.getMetaData.getColumnName(columns)
      val value = row.getObject(columns)
      val dataType = row.getMetaData.getColumnType(columns)
      toArcaneType(dataType) match
        case Success(arcaneType) => toDataRow(row, columns - 1, DataCell(name, arcaneType, value) :: acc)
        case Failure(exception) => Failure(exception)

/**
 * Companion object for [[LazyQueryResult]].
 */
object LazyQueryResult {

  /**
   * The output type of the query result.
   */
  type OutputType = LazyList[DataRow]

  /**
   * Creates a new [[LazyQueryResult]] object.
   *
   * @param statement The statement used to execute the query.
   * @param resultSet The result set of the query.
   * @return The new [[LazyQueryResult]] object.
   */
  def apply(statement: Statement, resultSet: ResultSet): LazyQueryResult = new LazyQueryResult(statement, resultSet, List.empty)
}
