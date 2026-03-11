package com.sneaksanddata.arcane.framework
package tests.shared

import models.ddl.CreateTableRequest
import models.schemas.ArcaneType.StringType
import models.schemas.{ArcaneSchema, Field}
import models.settings.iceberg.IcebergCatalogSettings
import models.settings.sink.SinkSettings
import services.iceberg.{
  IcebergS3CatalogWriter,
  IcebergSinkEntityManager,
  IcebergSinkTablePropertyManager,
  IcebergTablePropertyManager,
  given_Conversion_ArcaneSchema_Schema
}
import services.streaming.base.JsonWatermark

import zio.{Task, ZIO}

import java.time.{Instant, OffsetDateTime, ZoneOffset}

class IcebergUtil(catalogSettings: IcebergCatalogSettings):
  val propertyManager: IcebergSinkTablePropertyManager = IcebergSinkTablePropertyManager(
    catalogSettings
  )
  val entityManager: IcebergSinkEntityManager = IcebergSinkEntityManager(catalogSettings)

  val writer: IcebergS3CatalogWriter = IcebergS3CatalogWriter(entityManager, TestStagingSettings())

  def prepareWatermark(tableName: String, value: JsonWatermark): Task[Unit] =
    for
      targetName <- ZIO.succeed(tableName)
      // prepare target table metadata
      watermarkTime <- ZIO.succeed(OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC).minusHours(3))
      _ <- entityManager.createTable(CreateTableRequest(targetName, ArcaneSchema(Seq(Field("test", StringType))), true))
      _ <- propertyManager.comment(targetName, value.toJson)
    yield ()

object IcebergUtil:
  def apply(catalogSettings: IcebergCatalogSettings): IcebergUtil = new IcebergUtil(catalogSettings)
