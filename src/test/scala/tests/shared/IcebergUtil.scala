package com.sneaksanddata.arcane.framework
package tests.shared

import models.ddl.CreateTableRequest
import models.schemas.ArcaneType.StringType
import models.schemas.{ArcaneSchema, Field}
import models.settings.iceberg.IcebergCatalogSettings
import models.settings.sink.SinkSettings
import services.iceberg.{
  IcebergCatalogFactory,
  IcebergS3CatalogWriter,
  IcebergSinkEntityManager,
  IcebergSinkTablePropertyManager,
  IcebergStagingEntityManager,
  IcebergTablePropertyManager,
  given_Conversion_ArcaneSchema_Schema
}
import services.streaming.base.JsonWatermark

import zio.{Task, ZIO}

import java.time.{Instant, OffsetDateTime, ZoneOffset}

class IcebergUtil(catalogSettings: IcebergCatalogSettings):
  def getSinkPropertyManager: Task[IcebergSinkTablePropertyManager] = ZIO.scoped {
    for
      factory <- IcebergCatalogFactory.live(catalogSettings)
      result = IcebergSinkTablePropertyManager(catalogSettings, factory)
    yield result
  }

  def getSinkEntityManager: Task[IcebergSinkEntityManager] = ZIO.scoped {
    for
      factory <- IcebergCatalogFactory.live(catalogSettings)
      result = IcebergSinkEntityManager(catalogSettings, factory)
    yield result
  }

  def getWriter: Task[IcebergS3CatalogWriter] = ZIO.scoped {
    for
      stagingSettings <- ZIO.succeed(TestStagingSettings())
      factory         <- IcebergCatalogFactory.live(stagingSettings.icebergCatalog)
      entityManager = IcebergStagingEntityManager(stagingSettings.icebergCatalog, factory)
      result        = IcebergS3CatalogWriter(entityManager, stagingSettings)
    yield result
  }

  def prepareWatermark(tableName: String, value: JsonWatermark): Task[Unit] =
    for
      targetName      <- ZIO.succeed(tableName)
      entityManager   <- getSinkEntityManager
      propertyManager <- getSinkPropertyManager
      // prepare target table metadata
      watermarkTime <- ZIO.succeed(OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC).minusHours(3))
      _ <- entityManager.createTable(CreateTableRequest(targetName, ArcaneSchema(Seq(Field("test", StringType))), true))
      _ <- propertyManager.comment(targetName, value.toJson)
    yield ()

object IcebergUtil:
  def apply(catalogSettings: IcebergCatalogSettings): IcebergUtil = new IcebergUtil(catalogSettings)
