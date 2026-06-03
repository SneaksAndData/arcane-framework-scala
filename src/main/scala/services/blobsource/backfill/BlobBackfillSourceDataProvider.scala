package com.sneaksanddata.arcane.framework
package services.blobsource.backfill

import models.settings.backfill.BackfillSettings
import models.settings.sources.SourceBufferingSettings
import models.sharding.{BootstrappedShard, DefaultBootstrappedShard}
import services.backfill.{DefaultBackfillSourceDataProvider, DefaultBackfillStateManager}
import services.blobsource.readers.BlobSourceReader
import services.blobsource.versioning.BlobSourceWatermark
import services.naming.NameGenerator
import services.streaming.throughput.base.ThroughputShaperBuilder

import zio.stream.ZStream
import zio.{Task, ZIO}

import java.time.{Instant, OffsetDateTime, ZoneOffset}

class BlobBackfillSourceDataProvider(
    dataProvider: BlobSourceReader,
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
  override protected def getBackfillStartWatermark(startTime: Option[OffsetDateTime]): Task[BlobSourceWatermark] =
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
      case Some(sources) => ZStream.fromIterable(sources).mapZIO(dataProvider.fileToBlob)
      case None          => dataProvider.getShards(backfillId, backfillStart, backfillEnd)
    )
      .mapZIO { sourceFile =>
        dataProvider
          .fileToStream(sourceFile)
          .flatMap { structuredStream =>
            for
              prefix            <- nameGenerator.getBackfillTablesPrefix
              backfillTableName <- nameGenerator.getBackfillTableName
              targetName        <- nameGenerator.getTargetTableFullName
            yield DefaultBootstrappedShard(
              shardStream = structuredStream,
              shardSourceEntityName = sourceFile.name,
              combinedTableName = backfillTableName,
              targetTableName = targetName,
              backfillId = backfillId
            )
          }
      }

  /** Most recent version of the dataset at a time when a backfill was initiated.
    */
  override def getSnapshotVersion: Task[BlobSourceWatermark] = dataProvider.getLatestVersion
