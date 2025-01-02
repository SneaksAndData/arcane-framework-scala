package com.sneaksanddata.arcane.framework
package services.mssql.query

import services.mssql.MsSqlQuery
import services.mssql.base.QueryResult

import java.sql.{Connection, ResultSet, Statement}
import scala.concurrent.{Future, blocking}

/**
 * A class that runs a query on a SQL database.
 * This class is intended to run a single query and traverse the results only once using the forward-only read-only
 * cursor.
 *
 */
class QueryRunner[Output, QueryResultType <: QueryResult[Output]]:
  
  /**
   * A factory for creating a QueryResult object from a statement and a result set.
   *
   * @tparam Output The type of the output of the query.
   */
  private type ResultFactory = (Statement, ResultSet) => QueryResultType
  
  private implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  /**
   * Executes the given query on the given connection.
   *
   * The QueryResult object produced by this method must not outlive the connection object passed to it.
   *
   * @param query The query to execute.
   * @param connection The connection to execute the query on.
   * @return The result of the query.
   */
  def executeQuery(query: MsSqlQuery, connection: Connection, resultFactory: ResultFactory): Future[QueryResultType] =
    Future {
    val statement = connection.createStatement()
    val resultSet = blocking {
      statement.executeQuery(query)
    }
    resultFactory(statement, resultSet)
  }


object QueryRunner:
  def apply[O, T <: QueryResult[O]](): QueryRunner[O, T] = new QueryRunner[O, T]()
  
