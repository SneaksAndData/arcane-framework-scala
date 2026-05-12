package com.sneaksanddata.arcane.framework
package tests.shared

import models.schemas.ArcaneSchema
import services.iceberg.base.{SinkEntityManager, StagingEntityManager}
import services.streaming.processors.batch_processors.streaming.SchemaMigrationProcessor

import zio.{Cached, ZIO, ZLayer}

object VoidSchemaMigrationProcessor:
  private def live(sinkEntityManager: SinkEntityManager, stagingEntityManager: StagingEntityManager) = for cachedRef <-
      Cached.manual(
        acquire = ZIO.succeed(ArcaneSchema.empty())
      )
  yield new SchemaMigrationProcessor(
    sinkEntityManager = sinkEntityManager,
    stagingEntityManager = stagingEntityManager,
    schemaCacheRef = cachedRef,
    schemaMigrationEnabled = false,
    isTargetInStaging = false
  )

  val layer: ZLayer[SinkEntityManager & StagingEntityManager, Nothing, SchemaMigrationProcessor] = ZLayer.scoped {
    for
      sinkEntityManager    <- ZIO.service[SinkEntityManager]
      stagingEntityManager <- ZIO.service[StagingEntityManager]
      processor            <- live(sinkEntityManager, stagingEntityManager)
    yield processor
  }
