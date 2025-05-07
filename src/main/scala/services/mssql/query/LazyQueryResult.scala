package com.sneaksanddata.arcane.framework
package services.mssql.query

import models.schemas.{DataCell, DataRow}
import services.mssql.base.{CanPeekHead, QueryResult, ResultSetOwner}
import services.mssql.query.LazyQueryResult.toDataRow
import services.mssql.{SqlDataCell, SqlDataRow, given_Conversion_SqlDataRow_DataRow}
import utils.SqlUtils.toArcaneType

import java.sql.{ResultSet, Statement}
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/** Lazy-list based implementation of [[QueryResult]].
  *
  * @param statement
  *   The statement used to execute the query.
  * @param resultSet
  *   The result set of the query.
  */
class LazyQueryResult(protected val statement: Statement, protected val resultSet: ResultSet, eagerHead: List[DataRow])
    extends QueryResult[LazyList[DataRow]]
    with CanPeekHead[LazyList[DataRow]]
    with ResultSetOwner:

  /** Reads the result of the query.
    *
    * @return
    *   The result of the query.
    */
  override def read: this.OutputType =
    val columns = resultSet.getMetaData.getColumnCount
    eagerHead.to(LazyList) #::: LazyList
      .continually(resultSet)
      .takeWhile(_.next())
      .map(row => {
        toDataRow(row, columns, List.empty) match {
          case Success(dataRow)   => dataRow
          case Failure(exception) => throw exception
        }
      })
      .map(implicitly)

  /** Peeks the head of the result of the SQL query mapped to an output type.
    *
    * @return
    *   The head of the result of the query.
    */
  def peekHead: QueryResult[this.OutputType] & CanPeekHead[this.OutputType] =
    new LazyQueryResult(statement, resultSet, read.headOption.toList)

/** Companion object for [[LazyQueryResult]].
  */
object LazyQueryResult {

  /** The output type of the query result.
    */
  type OutputType = LazyList[DataRow]

  /** Creates a new [[LazyQueryResult]] object.
    *
    * @param statement
    *   The statement used to execute the query.
    * @param resultSet
    *   The result set of the query.
    * @return
    *   The new [[LazyQueryResult]] object.
    */
  def apply(statement: Statement, resultSet: ResultSet): LazyQueryResult =
    new LazyQueryResult(statement, resultSet, List.empty)

  @tailrec
  def toDataRow(row: ResultSet, column: Int, acc: SqlDataRow): Try[SqlDataRow] =
    if column == 0 then Success(acc)
    else
      val name     = row.getMetaData.getColumnName(column)
      val value    = row.getObject(column)
      val dataType = row.getMetaData.getColumnType(column)

      val precision = row.getMetaData.getPrecision(column)
      val scale     = row.getMetaData.getScale(column)

      toArcaneType(dataType, precision, scale) match
        case Success(arcaneType) => toDataRow(row, column - 1, SqlDataCell(name, arcaneType, value) :: acc)
        case Failure(exception)  => Failure(exception)

}
