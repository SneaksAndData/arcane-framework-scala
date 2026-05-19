package com.sneaksanddata.arcane.framework
package services.backfill

import models.backfill.DefaultSourceBackfill
import models.ddl.CreateTableRequest
import models.schemas.ArcaneSchema
import models.sharding.{BootstrappedShard, CompletionShard, StagedShard}
import services.backfill.base.BackfillStateManager
import services.iceberg.base.{StagingEntityManager, StagingPropertyManager}
import services.iceberg.given_Conversion_ArcaneSchema_Schema

import upickle.ReadWriter
import zio.{Task, ZIO}

class DefaultBackfillStateManager(stagingEntityManager: StagingEntityManager,
                                  stagingPropertyManager: StagingPropertyManager,
                                  stagedBackfillTableName: String) extends BackfillStateManager:
  override type StateImpl = DefaultSourceBackfill
  
  override def commitState(state: StateImpl)(implicit rw: ReadWriter[StateImpl]): Task[Unit] =
    stagingPropertyManager.setProperty(stagedBackfillTableName, statePropertyName, upickle.write(state))

  override def readState(implicit rw: ReadWriter[StateImpl]): Task[StateImpl] =
    stagingPropertyManager.getProperty(stagedBackfillTableName, statePropertyName).map(DefaultSourceBackfill(_))
  
  override def prepareShardCommit(shard: BootstrappedShard, schema: ArcaneSchema): Task[String] = for
    tableName <- ZIO.succeed(s"${shard.shardId.replace("-", "_")}")
    _ <- stagingEntityManager.createTable(CreateTableRequest(tableName, schema, false))
  yield tableName

  override def addCombinedShard(completionShard: CompletionShard): Task[Unit] = for
    state <- readState.map(s => s.copy(combinedShards = s.combinedShards ++ Seq(completionShard)))
    _ <- commitState(state)
  yield ()

  override def commitStagedShard(shard: StagedShard): Task[StagedShard] =
    stagingPropertyManager.setProperty(shard.shardTableName, stagedShardPropertyName, "1").map(_ => shard)
