package com.sneaksanddata.arcane.framework
package services.naming

import models.app.PluginStreamContext
import models.settings.TableNaming.{isValid, parts}
import models.settings.sink.SinkSettings
import models.settings.{BackfillIdentifier, StreamIdentifier}
import models.sharding.SourceShard

import zio.{Task, ZIO, ZLayer}

final class DefaultNameGenerator(
    sinkSettings: SinkSettings,
    backfillId: BackfillIdentifier,
    streamId: StreamIdentifier
) extends NameGenerator:
  private val nameSafeStreamId   = streamId.replace("-", "_")
  private val nameSafeBackfillId = backfillId.replace("-", "_")

  override def getBackfillTableName: Task[String] = for
    prefix <- getBackfillTablesPrefix
    name <- ZIO
      .when(backfillId.isValid)(ZIO.succeed(s"${prefix}__$nameSafeBackfillId"))
      .flatMap(ZIO.getOrFailWith(new Throwable(s"Invalid backfillId: '$nameSafeBackfillId'")))
  yield name

  override def getBackfillTablesPrefix: Task[String] = ZIO.succeed(s"backfill__$nameSafeStreamId")

  override def getTargetTableName: Task[String] = ZIO.succeed(sinkSettings.targetTableFullName.parts.name)

  override def getTargetTableFullName: Task[String] = ZIO.succeed(sinkSettings.targetTableFullName)

  override def getShardTableName(shard: SourceShard): Task[String] = for
    prefix <- getBackfillTablesPrefix
    _ <- ZIO.when(shard.backfillId != backfillId)(
      ZIO.fail(new Throwable(s"Shard carries an unknown backfill identifier: ${shard.backfillId}"))
    )
    _ <- ZIO.unless(backfillId.isValid)(ZIO.fail(new Throwable(s"Invalid backfillId: '$backfillId'")))
  yield Seq(
    prefix,
    nameSafeBackfillId,
    "shard",
    shard.shardId
  ).mkString("__")

  override def getStagingTableName: Task[String] = ???

object DefaultNameGenerator:
  val layer = ZLayer {
    for
      context    <- ZIO.service[PluginStreamContext]
      streamId   <- context.streamId
      backfillId <- context.backfillId
    yield new DefaultNameGenerator(context.sink, backfillId, streamId)
  }
