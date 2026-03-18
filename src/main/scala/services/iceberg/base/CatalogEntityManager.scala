package com.sneaksanddata.arcane.framework
package services.iceberg.base

import models.ddl.CreateTableRequest
import models.schemas.ArcaneSchema

import org.apache.iceberg.Table
import zio.Task

trait CatalogEntityManager:
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
  def createTable(request: CreateTableRequest): Task[Unit]

  /** Deletes all tables with name matching the specified prefix
    * @return
    */
  def deleteTables(prefix: String): Task[Unit]

  /** Migrates the table from oldSchema to newSchema by adding missing fields. Not supported: type updates, renames and
    * deletion
    * @return
    */
  def migrateSchema(oldSchema: ArcaneSchema, newSchema: ArcaneSchema, tableName: String): Task[Unit]

  /** Retrieve catalog factory used by this entity manager
    * @return
    */
  def getTableRef(tableName: String): Task[Table]

  /** Check if a specified table exists in the catalog
    * @return
    */
  def tableExists(tableName: String): Task[Boolean]

/** Entity manager for sink catalog
  */
trait SinkEntityManager extends CatalogEntityManager

/** Entity manager for staging catalog
  */
trait StagingEntityManager extends CatalogEntityManager
