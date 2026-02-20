package com.sneaksanddata.arcane.framework
package services.iceberg

import models.settings.SinkSettings
import services.iceberg.base.TablePropertyManager

import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.*
import zio.{Task, ZIO, ZLayer}

import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions

final class IcebergTablePropertyManager(sinkSettings: SinkSettings) extends TablePropertyManager:
  private val catalogFactory = new IcebergCatalogFactory(sinkSettings.icebergSinkSettings)

  private def loadTable(tableName: String, suffix: Option[String]): Task[Table] = for
    tableId <- ZIO.succeed(
      TableIdentifier.of(
        sinkSettings.icebergSinkSettings.namespace,
        suffix.map(v => s"$tableName.$v").getOrElse(tableName)
      )
    )
    catalog <- catalogFactory.getCatalog
    table <- ZIO
      .attemptBlocking(catalog.loadTable(catalogFactory.getSessionContext, tableId))
      .orDieWith(e => Throwable(s"Unable to load target table $tableName to read its properties", e))
  yield table

  override def comment(tableName: String, text: String): Task[Unit] = for
    table <- loadTable(tableName, None)
    _     <- ZIO.attemptBlocking(table.updateProperties().set("comment", text).commit())
  yield ()

  override def getProperty(tableName: String, propertyName: String): Task[String] = for
    table      <- loadTable(tableName, None)
    properties <- ZIO.attemptBlocking(table.properties())
  yield properties.get(propertyName)

  override def getTableSize(tableName: String): Task[(Records: Long, Size: Long)] = ZIO.scoped {
    for
      table <- loadTable(tableName, Some("partitions"))
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
      table <- loadTable(tableName, Some("partitions"))
      scanOps <- ZIO.acquireRelease(ZIO.attempt(table.newScan().planFiles())) { fileScans =>
        ZIO.attemptBlocking(fileScans.close()).orDie
      }
      result <- ZIO.foldLeft(scanOps.asScala)(0) { case (agg, _) =>
        ZIO.succeed(agg + 1)
      }
    yield result
  }

  override def getTableSchema(tableName: String): Task[Schema] = for table <- loadTable(tableName, None)
  yield table.schema()

object IcebergTablePropertyManager:

  type Environment = SinkSettings

  /** Factory method to create IcebergTablePropertyManager
    *
    * @param icebergSettings
    *   Iceberg settings
    * @return
    *   The initialized IcebergTablePropertyManager instance
    */
  def apply(icebergSettings: SinkSettings): IcebergTablePropertyManager =
    new IcebergTablePropertyManager(icebergSettings)

  /** The ZLayer that creates the IcebergTablePropertyManager.
    */
  val layer: ZLayer[Environment, Throwable, IcebergTablePropertyManager] =
    ZLayer {
      for settings <- ZIO.service[SinkSettings]
      yield IcebergTablePropertyManager(settings)
    }
