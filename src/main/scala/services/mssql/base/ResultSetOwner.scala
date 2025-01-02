package com.sneaksanddata.arcane.framework
package services.mssql.base

import java.sql.Statement

/**
 * AutoCloseable mixin for classes that own a result set.
 */
trait ResultSetOwner extends  AutoCloseable {
  protected val statement: Statement

  /**
   * Closes the statement and the result set owned by this object.
   * When a Statement object is closed, its current ResultSet object, if one exists, is also closed.
   */
  override def close(): Unit = statement.close()
}
