package com.sneaksanddata.arcane.framework
package services.iceberg

import logging.ZIOLogAnnotations.zlog
import models.ddl.CreateTableRequest
import models.schemas.ArcaneSchema
import models.settings.iceberg.{IcebergCatalogSettings, IcebergStagingSettings}
import models.settings.sink.SinkSettings
import services.iceberg.SchemaConversions.toIcebergType
import services.iceberg.base.{CatalogEntityManager, SinkEntityManager, StagingEntityManager}

import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.{PartitionSpec, SortOrder, Table}
import zio.{Task, ZIO, ZLayer}

import scala.jdk.CollectionConverters.*

trait IcebergEntityManager(catalogSettings: IcebergCatalogSettings) extends CatalogEntityManager:
  override val catalogFactory = new IcebergCatalogFactory(catalogSettings)

  private def delete(tableId: TableIdentifier): Task[Boolean] = for
    _       <- zlog("Deleting table %s", tableId.name())
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
    _ <- zlog("Found %s tables eligible for delete under prefix %s", matchingTables.size.toString, prefix)
    _ <- ZIO.foreachPar(matchingTables)(delete)
  yield ()

  override def migrateSchema(oldSchema: ArcaneSchema, newSchema: ArcaneSchema, tableName: String): Task[Unit] = for
    diff <- ZIO.succeed(oldSchema.getMissingFields(newSchema))
    _ <- ZIO.when(diff.nonEmpty) {
      for
        _ <- zlog(
          "Target schema of %s needs an update, will add the following fields: %s",
          tableName,
          diff.map(f => s"${f.name}/${f.fieldType.toString}").mkString(",")
        )
        tableId  <- ZIO.succeed(TableIdentifier.of(catalogSettings.namespace, tableName))
        catalog  <- catalogFactory.getCatalog
        tableRef <- ZIO.attemptBlockingIO(catalog.loadTable(catalogFactory.getSessionContext, tableId))
        updateBuilder <- ZIO.attempt(diff.foldLeft(tableRef.updateSchema()) { (builder, field) =>
          builder.addColumn(field.name, field.fieldType)
        })
        _ <- ZIO.attemptBlocking(updateBuilder.commit())
        _ <- zlog("Schema of %s successfully updated", tableName)
      yield ()
    }
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
    yield IcebergSinkEntityManager(sinkSettings.icebergCatalog)
  }

  val stagingLayer = ZLayer {
    for stagingSettings <- ZIO.service[IcebergStagingSettings]
    yield IcebergStagingEntityManager(stagingSettings)
  }
