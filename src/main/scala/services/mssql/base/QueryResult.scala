package com.sneaksanddata.arcane.framework
package services.mssql.base

/**
 * Represents the result of a query to a SQL database.
 */
trait QueryResult[Output] extends AutoCloseable: 

  /**
   * The output type of the query result.
   */
  type OutputType = Output

  /**
   * Reads the result of the SQL query mapped to an output type.
   *
   * @return The result of the query.
   */
  def read: Output

/**
 * Represents a query result that can peek the head of the result.
 *
 * @tparam Output The type of the output of the query.
 */
trait CanPeekHead[Output]:
  /**
   * Peeks the head of the result of the SQL query mapped to an output type.
   *
   * @return The head of the result of the query.
   */
  def peekHead: QueryResult[Output] & CanPeekHead[Output]
