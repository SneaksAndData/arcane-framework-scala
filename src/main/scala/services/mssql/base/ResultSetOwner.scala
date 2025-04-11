package com.sneaksanddata.arcane.framework
package services.mssql.base

import java.sql.{ResultSet, Statement}
import com.sneaksanddata.arcane.framework.services.mssql.MsSqlConnection.closeSafe

/**
 * AutoCloseable mixin for classes that own a result set.
 */
trait ResultSetOwner extends AutoCloseable:
  protected val statement: Statement
  protected val resultSet: ResultSet

  /**
   * Closes the statement and the result set owned by this object.
   * When a Statement object is closed, its current ResultSet object, if one exists, is also closed.
   */
  override def close(): Unit = resultSet.closeSafe(statement)
