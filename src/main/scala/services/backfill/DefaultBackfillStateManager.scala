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

  override def readState(implicit rw: ReadWriter[StateImpl]): Task[Option[StateImpl]] =
    stagingPropertyManager.getProperty(stagedBackfillTableName, statePropertyName).map(_.map(DefaultSourceBackfill(_)))
  
  override def prepareShardStage(shard: BootstrappedShard, schema: ArcaneSchema): Task[Unit] = for
    _ <- stagingEntityManager.createTable(CreateTableRequest(shard.shardTableName, schema, false))
  yield ()

  override def addCombinedShard(completionShard: CompletionShard): Task[Unit] = for
    state <- readState
    _ <- ZIO.when(state.isDefined) {
      for
        oldState <- ZIO.succeed(state.get)
        _ <- ZIO.unless(oldState.combinedShards.contains(completionShard)) {
          for
            newState <- ZIO.succeed(oldState.copy(combinedShards = oldState.combinedShards ++ Seq(completionShard)))
            _ <- commitState(newState)
          yield ()
        }
      yield ()
    }
  yield ()

  override def commitStagedShard(shard: StagedShard): Task[StagedShard] =
    stagingPropertyManager.setProperty(shard.shardTableName, stagedShardPropertyName, "1").map(_ => shard)

  override def isStaged(shard: BootstrappedShard): Task[Boolean] = for
    tableExists <- stagingEntityManager.tableExists(shard.shardTableName)
    result <- ZIO.when(tableExists)(stagingPropertyManager.getProperty(shard.shardTableName, stagedShardPropertyName).map(_.exists(_ == "1")))
  yield result.getOrElse(false)

  override def isCombined(shard: StagedShard): Task[Option[CompletionShard]] = for
    state <- readState
    result <- ZIO.when(state.isDefined) {
      for
        matchingShard <- ZIO.succeed(state.get.combinedShards.find(_.shardId == shard.shardId))
      yield matchingShard
    }
  yield result.flatten
