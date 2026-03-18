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

import zio.{Scope, Task, ZIO, ZLayer}

import java.time.{Instant, OffsetDateTime, ZoneOffset}

class IcebergUtil(catalogSettings: IcebergCatalogSettings):
  def getSinkTablePropertyManager: ZIO[Scope, Throwable, IcebergSinkTablePropertyManager] =
    for
      factory <- IcebergCatalogFactory.live(catalogSettings)
      result = IcebergSinkTablePropertyManager(catalogSettings, factory)
    yield result

  def getSinkTablePropertyManagerLayer: ZLayer[Any, Throwable, IcebergSinkTablePropertyManager] =
    ZLayer.scoped(getSinkTablePropertyManager)

  def getSinkEntityManager: ZIO[Scope, Throwable, IcebergSinkEntityManager] =
    for
      factory <- IcebergCatalogFactory.live(catalogSettings)
      result = IcebergSinkEntityManager(catalogSettings, factory)
    yield result

  def getSinkEntityManagerLayer: ZLayer[Any, Throwable, IcebergSinkEntityManager] = ZLayer.scoped(getSinkEntityManager)

  def getWriter: Task[IcebergS3CatalogWriter] = ZIO.scoped {
    for
      stagingSettings <- ZIO.succeed(TestStagingSettings())
      factory         <- IcebergCatalogFactory.live(stagingSettings.icebergCatalog)
      entityManager = IcebergStagingEntityManager(stagingSettings.icebergCatalog, factory)
      result        = IcebergS3CatalogWriter(entityManager, stagingSettings)
    yield result
  }

  def prepareWatermark(tableName: String, value: JsonWatermark): Task[Unit] = ZIO
    .scoped {
      for
        targetName      <- ZIO.succeed(tableName)
        entityManager   <- ZIO.service[IcebergSinkEntityManager]
        propertyManager <- ZIO.service[IcebergSinkTablePropertyManager]
        // prepare target table metadata
        watermarkTime <- ZIO.succeed(OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC).minusHours(3))
        _ <- entityManager.createTable(
          CreateTableRequest(targetName, ArcaneSchema(Seq(Field("test", StringType))), true)
        )
        _ <- propertyManager.comment(targetName, value.toJson)
      yield ()
    }
    .provide(getSinkTablePropertyManagerLayer, getSinkEntityManagerLayer)

object IcebergUtil:
  def apply(catalogSettings: IcebergCatalogSettings): IcebergUtil = new IcebergUtil(catalogSettings)
