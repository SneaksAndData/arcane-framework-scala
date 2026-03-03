package com.sneaksanddata.arcane.framework
package tests.shared

import models.schemas.ArcaneType.StringType
import models.schemas.{ArcaneSchema, Field}
import models.settings.iceberg.IcebergStagingSettings
import models.settings.sink.SinkSettings
import services.iceberg.{IcebergS3CatalogWriter, IcebergSinkEntityManager, IcebergTablePropertyManager, given_Conversion_ArcaneSchema_Schema}
import services.streaming.base.JsonWatermark

import zio.{Task, ZIO}

import java.time.{Instant, OffsetDateTime, ZoneOffset}

class IcebergUtil(sinkSettings: SinkSettings, stagingSettings: IcebergStagingSettings):
  val propertyManager: IcebergTablePropertyManager = IcebergTablePropertyManager(
    sinkSettings
  )
  val entityManager: IcebergSinkEntityManager = IcebergSinkEntityManager(sinkSettings.icebergSinkSettings)

  val writer: IcebergS3CatalogWriter = IcebergS3CatalogWriter(entityManager, stagingSettings)

  def prepareWatermark(tableName: String, value: JsonWatermark): Task[Unit] =
    for
      targetName <- ZIO.succeed(tableName)
      // prepare target table metadata
      watermarkTime <- ZIO.succeed(OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC).minusHours(3))
      _             <- entityManager.createTable(targetName, ArcaneSchema(Seq(Field("test", StringType))), true)
      _             <- propertyManager.comment(targetName, value.toJson)
    yield ()

object IcebergUtil:
  def apply(sinkSettings: SinkSettings, stagingSettings: IcebergStagingSettings): IcebergUtil =
    new IcebergUtil(sinkSettings, stagingSettings)
