package com.sneaksanddata.arcane.framework
package services.backfill

import logging.ZIOLogAnnotations.zlog
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

  override def prepareShardCombine(shard: StagedShard): Task[Unit] = for
    shardTableName <- nameGenerator.getShardTableName(shard)
    _ <- zlog("Opening COMBINE transaction for shard %s", shard.shardId)
    _ <- stagingPropertyManager.setProperty(
      shardTableName,
      processingStatePropertyName,
      ShardProcessingState.COMBINING.toString
    )
  yield ()  

  override def commitCombinedShard(completionShard: CompletionShard): Task[Unit] = for
    shardTableName <- nameGenerator.getShardTableName(completionShard)
    _ <- stagingPropertyManager.setProperty(
      shardTableName,
      processingStatePropertyName,
      ShardProcessingState.COMBINED.toString
    )
    _ <- stagingPropertyManager.setProperty(shardTableName, watermarkPropertyName, completionShard.watermark)
    _ <- zlog("Committed COMBINE transaction for shard %s", completionShard.shardId)
    _ <- ZIO.succeed(1) @@ declaredMetrics.backfillCombinedShards
  yield ()

  override def commitStagedShard(shard: StagedShard): Task[Unit] = for
    shardTableName <- nameGenerator.getShardTableName(shard)
    _ <- stagingPropertyManager.setProperty(
      shardTableName,
      processingStatePropertyName,
      ShardProcessingState.STAGED.toString
    )
    _ <- ZIO.succeed(1) @@ declaredMetrics.backfillStagedShards
  yield ()

  override def isStaged(shard: BootstrappedShard): Task[Boolean] = for
    shardTableName <- nameGenerator.getShardTableName(shard)
    tableExists    <- stagingEntityManager.tableExists(shardTableName)
    result <- ZIO.when(tableExists)(
      stagingPropertyManager
        .getProperty(shardTableName, processingStatePropertyName)
        .map(
          _.exists(state =>
            state == ShardProcessingState.STAGED.toString || state == ShardProcessingState.COMBINING.toString || state == ShardProcessingState.COMBINED.toString
          )
        )
    )
  yield result.getOrElse(false)

  override def isCombining(shard: StagedShard): Task[Boolean] = for
    shardTableName <- nameGenerator.getShardTableName(shard)
    tableExists    <- stagingEntityManager.tableExists(shardTableName)
    result <- ZIO.when(tableExists)(
      stagingPropertyManager
        .getProperty(shardTableName, processingStatePropertyName)
        .map(
          _.exists(_ == ShardProcessingState.COMBINING.toString)
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
