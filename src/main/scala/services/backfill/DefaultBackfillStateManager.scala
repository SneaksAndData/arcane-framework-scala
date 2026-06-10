package com.sneaksanddata.arcane.framework
package services.backfill

import models.backfill.DefaultSourceBackfill
import models.ddl.CreateTableRequest
import models.schemas.ArcaneSchema
import models.sharding.{BootstrappedShard, CompletionShard, StagedShard}
import services.backfill.base.{BackfillStateManager, ShardFactory, ShardProcessingState}
import services.iceberg.base.{StagingEntityManager, StagingPropertyManager}
import services.iceberg.given_Conversion_ArcaneSchema_Schema
import services.metrics.DeclaredMetrics
import services.naming.NameGenerator

import zio.{Task, ZIO, ZLayer}

class DefaultBackfillStateManager(
    stagingEntityManager: StagingEntityManager,
    stagingPropertyManager: StagingPropertyManager,
    shardFactory: ShardFactory,
    nameGenerator: NameGenerator,
    declaredMetrics: DeclaredMetrics
) extends BackfillStateManager:
  override type StateImpl = DefaultSourceBackfill

  override def commitState(state: StateImpl): Task[Unit] =
    nameGenerator.getBackfillTableName.flatMap(stagedBackfillTableName =>
      stagingPropertyManager.setProperty(stagedBackfillTableName, statePropertyName, upickle.write(state))
    )

  override def readState: Task[Option[StateImpl]] =
    nameGenerator.getBackfillTableName.flatMap(stagedBackfillTableName =>
      stagingPropertyManager.getProperty(stagedBackfillTableName, statePropertyName).map(_.map(upickle.read(_)))
    )

  override def prepareShardStage(shard: BootstrappedShard, schema: ArcaneSchema): Task[Unit] = for
    shardTableName <- nameGenerator.getShardTableName(shard)
    // replace = true ensures we do not append to the previously created shard table, in case a backfill was interrupted
    _ <- stagingEntityManager.createTable(CreateTableRequest(shardTableName, schema, true))
  yield ()

  override def commitCombinedShard(completionShard: CompletionShard): Task[CompletionShard] =
    nameGenerator.getShardTableName(completionShard).flatMap { shardTableName =>
      stagingPropertyManager
        .setProperty(shardTableName, processingStatePropertyName, ShardProcessingState.COMBINED.toString)
        .flatMap(_ =>
          stagingPropertyManager
            .setProperty(shardTableName, watermarkPropertyName, completionShard.watermark)
        )
        .flatMap { _ =>
          for _ <- ZIO.succeed(1) @@ declaredMetrics.backfillCombinedShards
          yield completionShard
        }
    }

  override def commitStagedShard(shard: StagedShard): Task[StagedShard] =
    nameGenerator.getShardTableName(shard).flatMap { shardTableName =>
      stagingPropertyManager
        .setProperty(shardTableName, processingStatePropertyName, ShardProcessingState.STAGED.toString)
        .flatMap { _ =>
          for _ <- ZIO.succeed(1) @@ declaredMetrics.backfillStagedShards
          yield shard
        }
    }

  override def isStaged(shard: BootstrappedShard): Task[Boolean] = for
    shardTableName <- nameGenerator.getShardTableName(shard)
    tableExists    <- stagingEntityManager.tableExists(shardTableName)
    result <- ZIO.when(tableExists)(
      stagingPropertyManager
        .getProperty(shardTableName, processingStatePropertyName)
        .map(
          _.exists(state =>
            state == ShardProcessingState.STAGED.toString || state == ShardProcessingState.COMBINED.toString
          )
        )
    )
  yield result.getOrElse(false)

  override def isCombined(shard: StagedShard): Task[Option[CompletionShard]] = for
    shardTableName <- nameGenerator.getShardTableName(shard)
    hasState <- stagingPropertyManager
      .getProperty(shardTableName, processingStatePropertyName)
      .map(_.exists(_ == ShardProcessingState.COMBINED.toString))
    watermark <- ZIO
      .when(hasState)(
        stagingPropertyManager
          .getProperty(shardTableName, watermarkPropertyName)
      )
      .map(_.flatten)
    result <- ZIO.when(watermark.isDefined)(shardFactory.createCompletionShard(shard, watermark.get))
  yield result

object DefaultBackfillStateManager:
  val layer = ZLayer {
    for
      stagingEntityManager   <- ZIO.service[StagingEntityManager]
      stagingPropertyManager <- ZIO.service[StagingPropertyManager]
      shardFactory           <- ZIO.service[ShardFactory]
      nameGenerator          <- ZIO.service[NameGenerator]
      declaredMetrics        <- ZIO.service[DeclaredMetrics]
    yield new DefaultBackfillStateManager(
      stagingEntityManager = stagingEntityManager,
      stagingPropertyManager = stagingPropertyManager,
      shardFactory = shardFactory,
      nameGenerator = nameGenerator,
      declaredMetrics = declaredMetrics
    )
  }
