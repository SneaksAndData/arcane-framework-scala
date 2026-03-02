package com.sneaksanddata.arcane.framework
package services.iceberg

import models.settings.{IcebergCatalogSettings, IcebergStagingSettings, SinkSettings}
import services.iceberg.base.{CatalogEntityManager, SinkEntityManager, StagingEntityManager}

import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.{PartitionSpec, Schema, Table}
import zio.{Task, ZIO, ZLayer}

trait IcebergEntityManager(catalogSettings: IcebergCatalogSettings) extends CatalogEntityManager:
  override val catalogFactory = new IcebergCatalogFactory(catalogSettings)
  /** Deletes the specified table from the catalog
   *
   * @param tableName
   * Table to delete
   * @return
   * true if successful, false otherwise
   */
  override def delete(tableName: String): Task[Boolean] = for
    tableId <- ZIO.succeed(TableIdentifier.of(catalogSettings.namespace, tableName))
    catalog <- catalogFactory.getCatalog
    result  <- ZIO.attemptBlocking(catalog.dropTable(catalogFactory.getSessionContext, tableId))
  yield result

  // TODO: create needs a separate class with create properties
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
  override def createTable(name: String, schema: Schema, replace: Boolean): Task[Table] = for
    tableId <- ZIO.succeed(TableIdentifier.of(catalogSettings.namespace, name))
    catalog <- catalogFactory.getCatalog
    tableBuilder <- ZIO.attempt(
      catalog
            .buildTable(catalogFactory.getSessionContext, tableId, schema)
            .withPartitionSpec(PartitionSpec.unpartitioned())
    )
    replacedRef <- ZIO.when(replace) {
      for
        _      <- ZIO.attemptBlocking(tableBuilder.createOrReplaceTransaction().commitTransaction())
        newRef <- ZIO.attemptBlocking(catalog.loadTable(catalogFactory.getSessionContext, tableId))
      yield newRef
    }
    tableRef <- ZIO.unless(replace) {
      for newRef <- ZIO.attemptBlocking(tableBuilder.create())
        yield newRef
    }
  yield replacedRef match
    case Some(ref) => ref
    case None      => tableRef.get
    
class IcebergSinkEntityManager(catalogSettings: IcebergCatalogSettings) extends IcebergEntityManager(catalogSettings) with SinkEntityManager

class IcebergStagingEntityManager(catalogSettings: IcebergCatalogSettings) extends IcebergEntityManager(catalogSettings) with StagingEntityManager

object IcebergEntityManager:
  val sinkLayer = ZLayer {
    for 
      sinkSettings <- ZIO.service[SinkSettings]
    yield IcebergSinkEntityManager(sinkSettings.icebergSinkSettings)  
  }
  
  val stagingLayer = ZLayer {
    for
      stagingSettings <- ZIO.service[IcebergStagingSettings]
    yield IcebergStagingEntityManager(stagingSettings) 
  }
  