package com.sneaksanddata.arcane.framework
package services.backfill

import models.backfill.DefaultSourceBackfill
import models.ddl.CreateTableRequest
import models.schemas.ArcaneSchema
import models.sharding.{BootstrappedShard, CompletionShard, StagedShard}
import services.backfill.base.{BackfillStateManager, ShardProcessingState}
import services.iceberg.base.{StagingEntityManager, StagingPropertyManager}
import services.iceberg.given_Conversion_ArcaneSchema_Schema

import com.sneaksanddata.arcane.framework.services.streaming.base.JsonWatermark
import upickle.ReadWriter
import zio.{Task, ZIO}

class DefaultBackfillStateManager(stagingEntityManager: StagingEntityManager,
                                  stagingPropertyManager: StagingPropertyManager,
                                  stagedBackfillTableName: String) extends BackfillStateManager:
  override type StateImpl = DefaultSourceBackfill
  
  override def commitState(state: StateImpl)(implicit rw: ReadWriter[StateImpl]): Task[Unit] =
    stagingPropertyManager.setProperty(stagedBackfillTableName, statePropertyName, upickle.write(state))

  override def readState(implicit rw: ReadWriter[StateImpl]): Task[Option[StateImpl]] =
    stagingPropertyManager.getProperty(stagedBackfillTableName, statePropertyName).map(_.map(DefaultSourceBackfill(_)))
  
  override def prepareShardStage(shard: BootstrappedShard, schema: ArcaneSchema): Task[Unit] = for
    _ <- stagingEntityManager.createTable(CreateTableRequest(shard.shardTableName, schema, false))
  yield ()

  override def commitCombinedShard(completionShard: CompletionShard): Task[CompletionShard] =
    stagingPropertyManager.setProperty(completionShard.shardTableName, processingStatePropertyName, ShardProcessingState.COMBINED.toString).map(_ => completionShard)

  override def commitStagedShard(shard: StagedShard): Task[StagedShard] =
    stagingPropertyManager.setProperty(shard.shardTableName, processingStatePropertyName, ShardProcessingState.STAGED.toString).map(_ => shard)

  override def isStaged(shard: BootstrappedShard): Task[Boolean] = for
    tableExists <- stagingEntityManager.tableExists(shard.shardTableName)
    result <- ZIO.when(tableExists)(stagingPropertyManager.getProperty(shard.shardTableName, processingStatePropertyName).map(_.exists(_ == ShardProcessingState.STAGED.toString)))
  yield result.getOrElse(false)

  override def isCombined(shard: StagedShard): Task[Option[CompletionShard]] = for
    hasState <- stagingPropertyManager.getProperty(shard.shardTableName, processingStatePropertyName).map(_.exists(_ == ShardProcessingState.COMBINED.toString))
    result <- ZIO.when(hasState)(stagingPropertyManager.getProperty(shard.shardTableName, watermarkPropertyName).map(wm => wm.map(value => CompletionShard(
      value,
      shard.shardTableName,
      shard.targetTableName,
      shard.shardSourceEntityName,
      shard.combinedTableName
    ))))
  yield result.flatten
