package com.sneaksanddata.arcane.framework
package services.streaming.processors.batch_processors.streaming

import models.batches.{MergeableBatch, StagedVersionedBatch}
import models.schemas.ArcaneSchema
import models.settings.TableNaming.*
import services.iceberg.base.*
import services.iceberg.given_Conversion_Schema_ArcaneSchema
import services.streaming.base.StagedBatchProcessor

import zio.stream.ZPipeline
import zio.{Cached, Scope, Task, ZIO}

class SchemaMigrationProcessor(sinkEntityManager: SinkEntityManager, stagingEntityManager: StagingEntityManager, schemaCacheRef: Cached[Throwable, ArcaneSchema], schemaMigrationEnabled: Boolean, isTargetInStaging: Boolean) extends StagedBatchProcessor:

  private def alignSchemas(
                            batchSchema: ArcaneSchema,
                            entityManager: CatalogEntityManager,
                            targetName: String
                          ): Task[Unit] = for
    targetSchema <- schemaCacheRef.get
    needsUpdate <- entityManager.migrateSchema(targetSchema, batchSchema, targetName).map(_.nonEmpty)
    _ <- ZIO.when(needsUpdate)(schemaCacheRef.refresh)
  yield ()

  override def process(streamSchema: ArcaneSchema): ZPipeline[Any, Throwable, StagedVersionedBatch & MergeableBatch, StagedVersionedBatch & MergeableBatch] = ZPipeline.mapZIO { batch =>
    for
      _ <- ZIO.when(!batch.isEmpty && schemaMigrationEnabled) {
            for
              // for streams, we migrate sink table
              _ <- ZIO.unless(isTargetInStaging)(alignSchemas(streamSchema, sinkEntityManager, batch.targetTableName))
              // for backfills, we migrate staging table
              _ <- ZIO.when(isTargetInStaging)(alignSchemas(streamSchema, stagingEntityManager, batch.targetTableName))
            yield ()
      }
    yield batch 
  }

object SchemaMigrationProcessor:
  private def getSchema(tableName: String, sinkPropertyManager: SinkPropertyManager): Task[ArcaneSchema] = sinkPropertyManager.getTableSchema(tableName).map(implicitly)
  /** Provide an instance of SchemaMigrationProcessor which supports manual refresh of a target schema cached value
   *
   * @return
   */
  def live(sinkEntityManager: SinkEntityManager, stagingEntityManager: StagingEntityManager, tableName: String, sinkPropertyManager: SinkPropertyManager, schemaMigrationEnabled: Boolean, isTargetInStaging: Boolean): ZIO[Scope, Throwable, SchemaMigrationProcessor] =
    for cachedRef <- Cached.manual(
      acquire = getSchema(tableName, sinkPropertyManager)
    )
    yield new SchemaMigrationProcessor(sinkEntityManager, stagingEntityManager, cachedRef, schemaMigrationEnabled, isTargetInStaging)