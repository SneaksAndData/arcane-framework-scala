package com.sneaksanddata.arcane.framework
package services.iceberg

import models.settings.IcebergSinkSettings
import services.iceberg.base.TablePropertyManager

import org.apache.iceberg.catalog.TableIdentifier
import zio.{Task, ZIO, ZLayer}

final class IcebergTablePropertyManager(sinkSettings: IcebergSinkSettings) extends TablePropertyManager:
  private val catalogFactory = new IcebergCatalogFactory(sinkSettings)
  
  override def comment(tableName: String, text: String): Task[Unit] = for
    tableId <- ZIO.succeed(TableIdentifier.of(sinkSettings.namespace, tableName))
    catalog <- catalogFactory.getCatalog
    table <- ZIO.attemptBlocking(catalog.loadTable(catalogFactory.getSessionContext, tableId))
    _ <- ZIO.attemptBlocking(table.updateProperties().set("comment", text).commit())
  yield ()
  
  override def getProperty(tableName: String, propertyName: String): Task[String] = for
    tableId <- ZIO.succeed(TableIdentifier.of(sinkSettings.namespace, tableName))
    catalog <- catalogFactory.getCatalog
    table <- ZIO
      .attemptBlocking(catalog.loadTable(catalogFactory.getSessionContext, tableId))
      .orDieWith(e => Throwable(s"Unable to load target table $tableName to read its properties", e))
    properties <- ZIO.attemptBlocking(table.properties())
  yield properties.get(propertyName)


object IcebergTablePropertyManager:

  type Environment = IcebergSinkSettings

  /** Factory method to create IcebergTablePropertyManager
   *
   * @param icebergSettings
   *   Iceberg settings
   * @return
   *   The initialized IcebergTablePropertyManager instance
   */
  def apply(icebergSettings: IcebergSinkSettings): IcebergTablePropertyManager =
    new IcebergTablePropertyManager(icebergSettings)

  /** The ZLayer that creates the LazyOutputDataProcessor.
   */
  val layer: ZLayer[Environment, Throwable, IcebergTablePropertyManager] =
    ZLayer {
      for settings <- ZIO.service[IcebergSinkSettings]
        yield IcebergTablePropertyManager(settings)
    }  