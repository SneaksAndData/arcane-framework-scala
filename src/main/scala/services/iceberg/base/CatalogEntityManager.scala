package com.sneaksanddata.arcane.framework
package services.iceberg.base

import models.ddl.CreateTableRequest

import org.apache.iceberg.Table
import zio.Task

trait CatalogEntityManager:
  /** Catalog connection factory
    */
  val catalogFactory: CatalogFactory

  /** Deletes the specified table from the catalog
    *
    * @param tableName
    *   Table to delete
    * @return
    *   true if successful, false otherwise
    */
  def delete(tableName: String): Task[Boolean]

  /** Creates a new table in the Iceberg catalog, using the provided schema
    * @return
    *   An Iceberg table reference
    */
  def createTable(request: CreateTableRequest): Task[Table]

  /**
   * Deletes all tables with name matching the specified prefix
   * @return
   */
  def deleteTables(prefix: String): Task[Unit]

/** Entity manager for sink catalog
  */
trait SinkEntityManager extends CatalogEntityManager

/** Entity manager for staging catalog
  */
trait StagingEntityManager extends CatalogEntityManager
