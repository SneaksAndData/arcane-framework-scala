package com.sneaksanddata.arcane.framework
package services.iceberg

import models.app.PluginStreamContext
import models.settings.iceberg.IcebergCatalogSettings
import services.iceberg.base.{SinkPropertyManager, StagingPropertyManager, TablePropertyManager}

import org.apache.iceberg.*
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.parquet.ParquetUtil
import org.apache.parquet.format.converter.ParquetMetadataConverter
import zio.{Task, ZIO, ZLayer}

import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions

trait IcebergTablePropertyManager(catalogSettings: IcebergCatalogSettings, catalogFactory: IcebergCatalogFactory)
    extends TablePropertyManager:

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

  private def loadMetadataTable(tableName: String, tableType: MetadataTableType): Task[Table] = for
    table <- loadTable(tableName)
    metadataTable <- ZIO.attemptBlocking(
      MetadataTableUtils.createMetadataTableInstance(table, tableType)
    )
  yield metadataTable

  override def comment(tableName: String, text: String): Task[Unit] = for
    table <- loadTable(tableName)
    _     <- ZIO.attemptBlocking(table.updateProperties().set("comment", text).commit())
  yield ()

  override def getProperty(tableName: String, propertyName: String): Task[String] = for
    table      <- loadTable(tableName)
    properties <- ZIO.attemptBlocking(table.properties())
  yield properties.get(propertyName)

  private def getTableMetadataScan(tableName: String, tableType: MetadataTableType) = for
    table <- loadMetadataTable(tableName, tableType)
    scanOps <- ZIO.acquireRelease(ZIO.attempt(table.newScan().planFiles())) { fileScans =>
      ZIO.attemptBlocking(fileScans.close()).orDie
    }
  yield scanOps

  override def getTableSize(tableName: String): Task[(Records: Long, Size: Long)] = ZIO.scoped {
    for
      scanOps <- getTableMetadataScan(tableName, MetadataTableType.PARTITIONS)
      result <- ZIO.foldLeft(scanOps.asScala)((0L, 0L)) { case (agg, el) =>
        ZIO.succeed(agg._1 + el.file.recordCount(), agg._2 + el.file().fileSizeInBytes())
      }
    yield result
  }

  override def getPartitionCount(tableName: String): Task[Int] = ZIO.scoped {
    for
      scanOps <- getTableMetadataScan(tableName, MetadataTableType.PARTITIONS)
      result <- ZIO.foldLeft(scanOps.asScala)(0) { case (agg, _) =>
        ZIO.succeed(agg + 1)
      }
    yield result
  }

  override def getColumnSizes(tableName: String): Task[Map[Int, Long]] = ZIO.scoped {
    for
      table   <- loadTable(tableName)
      scanOps <- ZIO.attempt(table.newScan().includeColumnStats().planFiles())
      result <- ZIO.foldLeft(scanOps.asScala)(Map.empty[Int, Long]) { case (agg, scanOp) =>
        val fieldSizes = Option(scanOp.file().columnSizes()) match {
          case Some(sizes) => sizes.asScala.map(v => (v._1.toInt, v._2.toLong))
          case None        => Map.empty
        }

        ZIO.succeed(fieldSizes.foldLeft(agg) { case (result, element) =>
          if !result.contains(element._1) then result ++ Map(element)
          else result ++ Map(element._1 -> (result(element._1) + element._2))
        })
      }
    yield result
  }

  override def getTableSchema(tableName: String): Task[Schema] = for table <- loadTable(tableName)
  yield table.schema()

class IcebergSinkTablePropertyManager(catalogSettings: IcebergCatalogSettings, catalogFactory: IcebergCatalogFactory)
    extends IcebergTablePropertyManager(catalogSettings, catalogFactory)
    with SinkPropertyManager

class IcebergStagingTablePropertyManager(catalogSettings: IcebergCatalogSettings, catalogFactory: IcebergCatalogFactory)
    extends IcebergTablePropertyManager(catalogSettings, catalogFactory)
    with StagingPropertyManager

object IcebergTablePropertyManager:

  val sinkLayer: ZLayer[PluginStreamContext, Throwable, IcebergSinkTablePropertyManager] =
    ZLayer.scoped {
      for
        context <- ZIO.service[PluginStreamContext]
        factory <- IcebergCatalogFactory.live(context.sink.icebergCatalog)
      yield IcebergSinkTablePropertyManager(context.sink.icebergCatalog, factory)
    }

  val stagingLayer: ZLayer[PluginStreamContext, Throwable, IcebergStagingTablePropertyManager] =
    ZLayer.scoped {
      for
        context <- ZIO.service[PluginStreamContext]
        factory <- IcebergCatalogFactory.live(context.staging.icebergCatalog)
      yield IcebergStagingTablePropertyManager(context.staging.icebergCatalog, factory)
    }
