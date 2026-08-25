package com.sneaksanddata.arcane.framework
package services.blobsource.readers.listing

import logging.ZIOLogAnnotations.{zlog, zlogStream}
import models.settings.sources.{DataRowModification, DataRowSchemaVersion}
import models.settings.{AllFieldsImpl, ExcludeFieldsImpl, FieldSelectionRuleSettings, IncludeFieldsImpl}
import models.schemas.{ArcaneSchema, DataRow, given_CanAdd_ArcaneSchema}
import services.blobsource.readers.BlobStreamingSource
import services.blobsource.versioning.BlobSourceWatermark
import services.naming.NameGenerator
import services.storage.base.{BlobStorageReader, BlobStorageWriter}
import services.storage.models.base.{BlobPath, StoredBlob}
import services.streaming.base.StructuredZStream

import zio.stream.{ZSink, ZStream}
import zio.{Chunk, Task, ZIO}

import java.security.MessageDigest
import java.time.OffsetDateTime
import java.util.UUID

abstract class BlobListingStreamingSource[PathType <: BlobPath](
    sourcePath: PathType,
    shardStoragePath: PathType,
    storageClient: BlobStorageReader[PathType] & BlobStorageWriter[PathType],
    nameGenerator: NameGenerator,
    primaryKeys: Seq[String],
    tempStoragePath: String,
    fieldSelector: FieldSelectionRuleSettings,
    modifications: Seq[DataRowModification],
    dataRowSchemaVersion: DataRowSchemaVersion
) extends BlobStreamingSource(modifications, dataRowSchemaVersion):

  protected val parallelism: Int                            = Runtime.getRuntime.availableProcessors()
  override protected val primaryKeyNames: Task[Seq[String]] = ZIO.succeed(primaryKeys)

  override def fileToBlob(sourceFile: String): Task[StoredBlob] = storageClient.blobMetadata(sourceFile)

  final override def deleteShards(prefix: String): Task[Unit] = storageClient
    .streamPrefixes(
      shardStoragePath + prefix
    )
    .mapZIO(file =>
      zlog("Deleting outdated shard: %s", file.name) *> storageClient.removeBlob(
        file.name
      )
    )
    .runDrain

  /** SHA-256 hasher.
    */
  protected def mergeKeyHasher(): MessageDigest = MessageDigest.getInstance("SHA-256")

  protected def downloadSourceFile(sourceFile: StoredBlob): Task[String] =
    storageClient.downloadBlob(s"${sourcePath.protocol}://${sourceFile.name}", tempStoragePath)

  override def getLatestVersion: Task[BlobSourceWatermark] = storageClient
    .streamPrefixes(sourcePath)
    .map(_.createdOn.getOrElse(0L))
    .run(ZSink.foldLeft(0L)((e, agg) => if (e > agg) e else agg))
    .map(BlobSourceWatermark.fromEpochSecond)

  override def getVersionRange(
      startFrom: BlobSourceWatermark,
      finishAt: BlobSourceWatermark
  ): ZStream[Any, Throwable, BlobSourceWatermark] = storageClient
    .streamPrefixes(sourcePath)
    .collect {
      case blob
          if BlobSourceWatermark.fromEpochSecond(blob.createdOn.getOrElse(0L)) >= startFrom && BlobSourceWatermark
            .fromEpochSecond(blob.createdOn.getOrElse(0L)) <= finishAt =>
        BlobSourceWatermark.fromEpochSecond(blob.createdOn.getOrElse(0L))
    }

  // due to the fact that this is always called by StreamingDataProvider after comparing versions
  // and the fact that versions are file creation dates, we can safely assume that IF this method is called, it will return TRUE. Hence no need to double list files
  override def hasChanges(previousVersion: BlobSourceWatermark): Task[Boolean] = ZIO.succeed(true)

  private def getEligibleFiles(
      rangeStart: BlobSourceWatermark,
      rangeEnd: BlobSourceWatermark
  ): ZStream[Any, Throwable, StoredBlob] = storageClient
    .streamPrefixes(sourcePath)
    .collect {
      case blob
          if blob.createdOn.map(BlobSourceWatermark.fromEpochSecond).getOrElse(BlobSourceWatermark.epoch) >= rangeStart
            && blob.createdOn
              .map(BlobSourceWatermark.fromEpochSecond)
              .getOrElse(BlobSourceWatermark.epoch) <= rangeEnd =>
        blob
    }

  private def estimateShardSize(rangeStart: BlobSourceWatermark, rangeEnd: BlobSourceWatermark): Task[Int] = for
    _ <- zlog(
      "Estimating shard size using 1000 files between %s and %s",
      rangeStart.timestamp.toString,
      rangeEnd.timestamp.toString
    )
    sample <- getEligibleFiles(rangeStart, rangeEnd).take(1000).runCollect
    // if file size cannot be determined, assume 100kb
    avgFileSize <- ZIO.succeed(sample.map(_.contentLength.getOrElse(1024L * 1024L * 100L)).sum / sample.size)
    _           <- zlog("Average file size for shards: %s bytes, max shard size is 10Mib", avgFileSize.toString)
  yield Seq((10L * 1024L * 1024L * 1024L / avgFileSize).toInt + 1, 10000).min

  final override def getShards(
      rangeStart: BlobSourceWatermark,
      rangeEnd: BlobSourceWatermark
  ): ZStream[Any, Throwable, Seq[String]] = ZStream
    .fromZIO(estimateShardSize(rangeStart, rangeEnd))
    .flatMap { shardSize =>
      zlogStream("Using shard size of %s files/shard", shardSize.toString) *> getEligibleFiles(rangeStart, rangeEnd)
        .rechunk(shardSize)
        .mapChunks(files => Chunk(files.map(_.name)))
        .rechunk(1)
    }

  override def persistShard(shardContent: String): Task[String] = for
    shardId   <- ZIO.succeed(UUID.randomUUID().toString)
    shardName <- nameGenerator.getShardSourceTableName(shardId)
    _         <- storageClient.saveTextAsBlob(shardStoragePath + shardName, shardContent)
  yield shardId

  override def readShard(shardSourceEntityName: String): Task[String] =
    for
      shardName <- nameGenerator.getShardSourceTableName(shardSourceEntityName)
      result    <- storageClient.readBlobContent(shardStoragePath + shardName)
    yield result

  override def getChanges(startFrom: BlobSourceWatermark): ZStream[Any, Throwable, StructuredZStream] = ZStream
    .fromZIO(getSchema)
    .flatMap { changeSetSchema =>
      storageClient
        .streamPrefixes(sourcePath)
        .filter(_.createdOn.map(BlobSourceWatermark.fromEpochSecond).getOrElse(BlobSourceWatermark.epoch) >= startFrom)
        // regroup files based on core count available
        .rechunk(parallelism * 10)
        .mapChunksZIO(files => filesToStream(files, changeSetSchema).map(stream => Chunk(stream)))
    }

  // in 2.4 release this will be integrated via DataRowModification and provided uniformly for all source
  // this code only addresses schema alignment issues in 2.3 release for non-server-side filtered sources.
  final protected def applyFieldSelector(schema: ArcaneSchema): ArcaneSchema =
    fieldSelector.rule match
      case AllFieldsImpl(_) => schema
      case IncludeFieldsImpl(includeFields) =>
        schema.filter(f =>
          includeFields.fields.exists(_.equalsIgnoreCase(f.name)) || fieldSelector.essentialFields.exists(
            _.equalsIgnoreCase(f.name)
          )
        )
      case ExcludeFieldsImpl(excludeFields) =>
        schema.filterNot(f => excludeFields.fields.exists(_.equalsIgnoreCase(f.name)))

  final protected def applyFieldSelector(row: DataRow): DataRow =
    fieldSelector.rule match
      case AllFieldsImpl(_) => row
      case IncludeFieldsImpl(includeFields) =>
        row.filter(cell =>
          includeFields.fields.exists(_.equalsIgnoreCase(cell.name)) || fieldSelector.essentialFields.exists(
            _.equalsIgnoreCase(cell.name)
          )
        )
      case ExcludeFieldsImpl(excludeFields) =>
        row.filterNot(cell => excludeFields.fields.exists(_.equalsIgnoreCase(cell.name)))
