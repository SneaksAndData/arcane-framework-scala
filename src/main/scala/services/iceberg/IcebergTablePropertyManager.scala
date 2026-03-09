package com.sneaksanddata.arcane.framework
package services.iceberg

import models.settings.iceberg.{IcebergCatalogSettings, IcebergStagingSettings}
import models.settings.sink.SinkSettings
import services.iceberg.base.{SinkPropertyManager, StagingPropertyManager, TablePropertyManager}

import com.sneaksanddata.arcane.framework.models.app.PluginStreamContext
import org.apache.iceberg.*
import org.apache.iceberg.catalog.TableIdentifier
import zio.{Task, ZIO, ZLayer}

import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions

trait IcebergTablePropertyManager(catalogSettings: IcebergCatalogSettings) extends TablePropertyManager:
  override val catalogFactory = new IcebergCatalogFactory(catalogSettings)

  private def loadTable(tableName: String): Task[Table] = for
    tableId <- ZIO.succeed(
      TableIdentifier.of(
        catalogSettings.namespace,
        tableName
      )
    )
    catalog <- catalogFactory.getCatalog
    table <- ZIO
      .attemptBlocking(catalog.loadTable(catalogFactory.getSessionContext, tableId))
      .orDieWith(e => Throwable(s"Unable to load target table $tableName to read its properties", e))
  yield table

  private def loadPartitionsTable(tableName: String): Task[Table] = for
    table <- loadTable(tableName)
    partitionsTable <- ZIO.attemptBlocking(
      MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.PARTITIONS)
    )
  yield partitionsTable

  override def comment(tableName: String, text: String): Task[Unit] = for
    table <- loadTable(tableName)
    _     <- ZIO.attemptBlocking(table.updateProperties().set("comment", text).commit())
  yield ()

  override def getProperty(tableName: String, propertyName: String): Task[String] = for
    table      <- loadTable(tableName)
    properties <- ZIO.attemptBlocking(table.properties())
  yield properties.get(propertyName)

  override def getTableSize(tableName: String): Task[(Records: Long, Size: Long)] = ZIO.scoped {
    for
      table <- loadPartitionsTable(tableName)
      scanOps <- ZIO.acquireRelease(ZIO.attempt(table.newScan().planFiles())) { fileScans =>
        ZIO.attemptBlocking(fileScans.close()).orDie
      }
      result <- ZIO.foldLeft(scanOps.asScala)((0L, 0L)) { case (agg, el) =>
        ZIO.succeed(agg._1 + el.file.recordCount(), agg._2 + el.file().fileSizeInBytes())
      }
    yield result
  }

  override def getPartitionCount(tableName: String): Task[Int] = ZIO.scoped {
    for
      table <- loadPartitionsTable(tableName)
      scanOps <- ZIO.acquireRelease(ZIO.attempt(table.newScan().planFiles())) { fileScans =>
        ZIO.attemptBlocking(fileScans.close()).orDie
      }
      result <- ZIO.foldLeft(scanOps.asScala)(0) { case (agg, _) =>
        ZIO.succeed(agg + 1)
      }
    yield result
  }

  override def getTableSchema(tableName: String): Task[Schema] = for table <- loadTable(tableName)
  yield table.schema()

class IcebergSinkTablePropertyManager(catalogSettings: IcebergCatalogSettings)
    extends IcebergTablePropertyManager(catalogSettings)
    with SinkPropertyManager

class IcebergStagingTablePropertyManager(catalogSettings: IcebergCatalogSettings)
    extends IcebergTablePropertyManager(catalogSettings)
    with StagingPropertyManager

object IcebergTablePropertyManager:

  val sinkLayer =
    ZLayer {
      for context <- ZIO.service[PluginStreamContext]
      yield IcebergSinkTablePropertyManager(context.sink.icebergCatalog)
    }

  val stagingLayer =
    ZLayer {
      for context <- ZIO.service[PluginStreamContext]
      yield IcebergStagingTablePropertyManager(context.staging.icebergCatalog)
    }
