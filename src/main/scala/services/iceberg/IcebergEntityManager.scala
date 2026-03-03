package com.sneaksanddata.arcane.framework
package services.iceberg

import models.ddl.CreateTableRequest
import models.settings.iceberg.{IcebergCatalogSettings, IcebergStagingSettings}
import models.settings.sink.SinkSettings
import services.iceberg.base.{CatalogEntityManager, SinkEntityManager, StagingEntityManager}

import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.{PartitionSpec, SortOrder, Table}
import zio.{Task, ZIO, ZLayer}

import scala.jdk.CollectionConverters.*

trait IcebergEntityManager(catalogSettings: IcebergCatalogSettings) extends CatalogEntityManager:
  override val catalogFactory = new IcebergCatalogFactory(catalogSettings)

  private def delete(tableId: TableIdentifier): Task[Boolean] = for
    catalog <- catalogFactory.getCatalog
    result  <- ZIO.attemptBlocking(catalog.dropTable(catalogFactory.getSessionContext, tableId))
  yield result

  /** Deletes the specified table from the catalog
    *
    * @param tableName
    *   Table to delete
    * @return
    *   true if successful, false otherwise
    */
  override def delete(tableName: String): Task[Boolean] = for
    tableId <- ZIO.succeed(TableIdentifier.of(catalogSettings.namespace, tableName))
    result  <- delete(tableId)
  yield result

  /** Creates a new table in the Iceberg catalog, using the provided schema
    * @return
    */
  override def createTable(request: CreateTableRequest): Task[Table] = for
    tableId <- ZIO.succeed(TableIdentifier.of(catalogSettings.namespace, request.name))
    catalog <- catalogFactory.getCatalog
    tableBuilder <- ZIO.attempt(
      catalog
        .buildTable(catalogFactory.getSessionContext, tableId, request.schema)
        .withPartitionSpec(request.partitionSpec.getOrElse(PartitionSpec.unpartitioned()))
        .withSortOrder(request.sortOrder.getOrElse(SortOrder.unsorted()))
    )
    replacedRef <- ZIO.when(request.replace) {
      for
        _      <- ZIO.attemptBlocking(tableBuilder.createOrReplaceTransaction().commitTransaction())
        newRef <- ZIO.attemptBlocking(catalog.loadTable(catalogFactory.getSessionContext, tableId))
      yield newRef
    }
    tableRef <- ZIO.unless(request.replace) {
      for newRef <- ZIO.attemptBlocking(tableBuilder.create())
      yield newRef
    }
  yield replacedRef match
    case Some(ref) => ref
    case None      => tableRef.get

  override def deleteTables(prefix: String): Task[Unit] = for
    catalog <- catalogFactory.getCatalog
    matchingTables <- ZIO
      .attemptBlockingIO(catalog.listTables(catalogFactory.getSessionContext, Namespace.of(catalogSettings.namespace)))
      .map(_.asScala.filter(_.name().startsWith(prefix)).toList)
    _ <- ZIO.foreachPar(matchingTables)(delete)
  yield ()

class IcebergSinkEntityManager(catalogSettings: IcebergCatalogSettings)
    extends IcebergEntityManager(catalogSettings)
    with SinkEntityManager

class IcebergStagingEntityManager(catalogSettings: IcebergCatalogSettings)
    extends IcebergEntityManager(catalogSettings)
    with StagingEntityManager

object IcebergEntityManager:
  val sinkLayer = ZLayer {
    for sinkSettings <- ZIO.service[SinkSettings]
    yield IcebergSinkEntityManager(sinkSettings.icebergSinkSettings)
  }

  val stagingLayer = ZLayer {
    for stagingSettings <- ZIO.service[IcebergStagingSettings]
    yield IcebergStagingEntityManager(stagingSettings)
  }
