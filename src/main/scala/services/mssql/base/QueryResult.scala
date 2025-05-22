package com.sneaksanddata.arcane.framework
package services.mssql.base

/** Represents the result of a query to a SQL database.
  */
trait QueryResult[Output]:

  /** The output type of the query result.
    */
  type OutputType = Output

  /** Reads the result of the SQL query mapped to an output type.
    *
    * @return
    *   The result of the query.
    */
  def read: Output
