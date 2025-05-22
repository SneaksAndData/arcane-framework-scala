package com.sneaksanddata.arcane.framework
package services.mssql.query

import com.sneaksanddata.arcane.framework.models.schemas.DataRow
import com.sneaksanddata.arcane.framework.services.mssql.{SqlDataCell, SqlDataRow}
import com.sneaksanddata.arcane.framework.utils.SqlUtils.toArcaneType
import zio.{Scope, UIO, ZIO}
import zio.stream.ZStream

import java.sql.{ResultSet, Statement}
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object MsSqlResultSet:
  extension (resultSet: ResultSet) 
    def toZStream: ZStream[Any, Throwable, SqlDataRow] = ZStream.unfoldZIO(resultSet.next()) { hasNext =>
      if hasNext then
        for
          columns <- ZIO.attemptBlockingInterrupt(resultSet.getMetaData.getColumnCount)
          row <- ZIO.fromTry(toDataRow(resultSet, columns, List.empty))
          hasNextRow <- ZIO.attemptBlockingInterrupt(resultSet.next())
        yield Some((row, hasNextRow))
      else ZIO.succeed(None)
    }

  @tailrec
  private def toDataRow(resultSet: ResultSet, column: Int, acc: SqlDataRow): Try[SqlDataRow] =
    if column == 0 then Success(acc)
    else
      val name = resultSet.getMetaData.getColumnName(column)
      val value = resultSet.getObject(column)
      val dataType = resultSet.getMetaData.getColumnType(column)

      val precision = resultSet.getMetaData.getPrecision(column)
      val scale = resultSet.getMetaData.getScale(column)

      toArcaneType(dataType, precision, scale) match
        case Success(arcaneType) => toDataRow(resultSet, column - 1, SqlDataCell(name, arcaneType, value) :: acc)
        case Failure(exception) => Failure(exception)

  /** Closes the result in a safe way. MsSQL JDBC driver enforces the result set to iterate over all the rows returned
   * by the query if the result set is being closed without cancelling the statement first. see:
   * https://github.com/microsoft/mssql-jdbc/issues/877 for details. ALL RESULT SETS CREATED FROM MS SQL CONNECTION
   * MUST BE CLOSED THIS WAY
   *
   * @param resultSet
   * The result set to close.
   * @param statement
   * The statement to close.
   * @return
   * UIO[Unit] that completes when the result set is closed.
   */
  extension (resultSet: ResultSet)
    def closeSafe(statement: Statement): UIO[Unit] =
      for
        _ <- ZIO.succeed(statement.cancel())
        _ <- ZIO.succeed(resultSet.close())
      yield ()

  /** Closes the result in a safe way. MsSQL JDBC driver enforces the result set to iterate over all the rows returned
   * by the query if the result set is being closed without cancelling the statement first. see:
   * https://github.com/microsoft/mssql-jdbc/issues/877 for details. ALL RESULT SETS CREATED FROM MS SQL CONNECTION
   * MUST BE CLOSED THIS WAY
   *
   * @param resultSet
   * The result set to close.
   * @param statement
   * The statement to close.
   * @return
   * Scoped effect that tracks the result set and closes it when the effect is completed.
   */
  extension (statement: Statement)
    def executeQuerySafe(query: String): ZIO[Scope, Throwable, ResultSet] =
      for resultSet <- ZIO.acquireRelease(ZIO.attemptBlocking(statement.executeQuery(query)))(rs =>
        rs.closeSafe(statement)
      )
      yield resultSet      
