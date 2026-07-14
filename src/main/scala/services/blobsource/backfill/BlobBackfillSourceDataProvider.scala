package com.sneaksanddata.arcane.framework
package services.blobsource.backfill

import models.app.PluginStreamContext
import models.settings.backfill.BackfillSettings
import models.settings.sources.SourceBufferingSettings
import models.sharding.{BootstrappedShard, DefaultBootstrappedShard}
import services.backfill.{DefaultBackfillSourceDataProvider, DefaultBackfillStateManager}
import services.blobsource.readers.BlobStreamingSource
import services.blobsource.versioning.BlobSourceWatermark
import services.naming.NameGenerator
import services.streaming.throughput.base.ThroughputShaperBuilder

import zio.stream.{ZPipeline, ZSink, ZStream}
import zio.{Chunk, Task, ZIO, ZLayer}

import java.time.{Instant, OffsetDateTime, ZoneOffset}

class BlobBackfillSourceDataProvider(
    dataProvider: BlobStreamingSource,
    backfillSettings: BackfillSettings,
    stateManager: DefaultBackfillStateManager,
    throughputShaperBuilder: ThroughputShaperBuilder,
    sourceBufferingSettings: SourceBufferingSettings,
    nameGenerator: NameGenerator,
    backfillId: String
) extends DefaultBackfillSourceDataProvider[BlobSourceWatermark](
      dataProvider,
      backfillSettings,
      throughputShaperBuilder,
      sourceBufferingSettings,
      stateManager
    ):
  /** Evaluates watermark to be used when evaluating current snapshot version at the start of a backfill process
    *
    * @return
    */
  override def getBackfillStartWatermark(startTime: Option[OffsetDateTime]): Task[BlobSourceWatermark] =
    ZIO.succeed(
      BlobSourceWatermark.fromEpochSecond(
        startTime.getOrElse(OffsetDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC)).toInstant.toEpochMilli / 1000
      )
    )

  /** Implements data streaming logic for public `requestBackfill`
    *
    * @return
    */
  override protected def backfillStream(
      backfillStart: BlobSourceWatermark,
      backfillEnd: BlobSourceWatermark,
      shardSources: Option[Seq[String]]
  ): ZStream[Any, Throwable, BootstrappedShard] =
    (shardSources match
      case Some(sources) => ZStream.fromIterable(sources).mapZIO { source =>
        for
          content <- dataProvider.readShard(source)
          unpacked <- dataProvider.unpackShard(content)
        yield unpacked 
      }
      case None          => dataProvider.getShards(backfillStart, backfillEnd)
    )
      .mapZIO { sourceFiles =>
        for
          blobs             <- ZStream.fromIterable(sourceFiles).mapZIO(dataProvider.fileToBlob).runCollect
          shardStream       <- dataProvider.filesToStream(blobs)
          backfillTableName <- nameGenerator.getBackfillTableName
          targetName        <- nameGenerator.getTargetTableFullName
          shardSourceContent   <- dataProvider.packShard(sourceFiles)
          shardSourceEntityName <- dataProvider.persistShard(shardSourceContent)
        yield DefaultBootstrappedShard(
          shardStream = shardStream,
          shardSourceEntityName = shardSourceEntityName,
          combinedTableName = backfillTableName,
          targetTableName = targetName,
          backfillId = backfillId
        )
      }

  /** Most recent version of the dataset at a time when a backfill was initiated.
    */
  override def getSnapshotVersion: Task[BlobSourceWatermark] = dataProvider.getLatestVersion

object BlobBackfillSourceDataProvider:
  val layer = ZLayer {
    for
      dataProvider            <- ZIO.service[BlobStreamingSource]
      context                 <- ZIO.service[PluginStreamContext]
      stateManager            <- ZIO.service[DefaultBackfillStateManager]
      throughputShaperBuilder <- ZIO.service[ThroughputShaperBuilder]
      nameGenerator           <- ZIO.service[NameGenerator]
      backfillId              <- context.backfillId
    yield new BlobBackfillSourceDataProvider(
      dataProvider = dataProvider,
      backfillSettings = context.streamMode.backfill,
      stateManager = stateManager,
      throughputShaperBuilder = throughputShaperBuilder,
      sourceBufferingSettings = context.source.buffering,
      nameGenerator = nameGenerator,
      backfillId = backfillId
    )
  }
