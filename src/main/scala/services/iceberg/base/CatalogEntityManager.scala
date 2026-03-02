package com.sneaksanddata.arcane.framework
package services.iceberg.base

import org.apache.iceberg.{Schema, Table}
import zio.Task

trait CatalogEntityManager:
  val catalogFactory: CatalogFactory
  /** Deletes the specified table from the catalog
   *
   * @param tableName
   * Table to delete
   * @return
   * true if successful, false otherwise
   */
  def delete(tableName: String): Task[Boolean]

  /** Creates a new table in the Iceberg catalog, using the provided schema
   *
   * @param name
   * Name for the table, excluding schema (namespace) name
   * @param schema
   * Schema for the table
   * @param replace
   * If true, will replace the table if it exists
   * @return
   */
  def createTable(name: String, schema: Schema, replace: Boolean): Task[Table]

/**
 * Entity manager for sink catalog
 */
trait SinkEntityManager extends CatalogEntityManager

/**
 * Entity manager for staging catalog
 */
trait StagingEntityManager extends CatalogEntityManager
