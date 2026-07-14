package com.sneaksanddata.arcane.framework
package services.blobsource.readers.listing

import logging.ZIOLogAnnotations.zlog
import models.schemas.{ArcaneSchema, given_CanAdd_ArcaneSchema}
import services.blobsource.readers.BlobStreamingSource
import services.blobsource.versioning.BlobSourceWatermark
import services.naming.NameGenerator
import services.storage.base.{BlobStorageReader, BlobStorageWriter}
import services.storage.models.base.{BlobPath, StoredBlob}
import services.streaming.base.StructuredZStream

import zio.stream.{ZSink, ZStream}
import zio.{Chunk, Task, ZIO}

import java.security.MessageDigest
import java.util.UUID

abstract class BlobListingStreamingSource[PathType <: BlobPath](
    sourcePath: PathType,
    shardStoragePath: PathType,
    storageClient: BlobStorageReader[PathType] & BlobStorageWriter[PathType],
    nameGenerator: NameGenerator,
    primaryKeys: Seq[String],
    tempStoragePath: String
) extends BlobStreamingSource:

  override def fileToBlob(sourceFile: String): Task[StoredBlob] = storageClient.blobMetadata(sourceFile)

  final override def deleteShards(prefix: String): Task[Unit] = storageClient
    .streamPrefixes(
      shardStoragePath + prefix
    )
    .mapZIO(file =>
      zlog("Deleting outdated shard: %s", (shardStoragePath + file.name).toHdfsPath) *> storageClient.removeBlob(
        shardStoragePath + file.name
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

  // due to the fact that this is always called by StreamingDataProvider after comparing versions
  // and the fact that versions are file creation dates, we can safely assume that IF this method is called, it will return TRUE. Hence no need to double list files
  override def hasChanges(previousVersion: BlobSourceWatermark): Task[Boolean] = ZIO.succeed(true)

  final override def getShards(
      rangeStart: BlobSourceWatermark,
      rangeEnd: BlobSourceWatermark
  ): ZStream[Any, Throwable, Seq[String]] = storageClient
    .streamPrefixes(sourcePath)
    .collect {
      case blob
          if blob.createdOn.map(BlobSourceWatermark.fromEpochSecond).getOrElse(BlobSourceWatermark.epoch) >= rangeStart
            && blob.createdOn
              .map(BlobSourceWatermark.fromEpochSecond)
              .getOrElse(BlobSourceWatermark.epoch) <= rangeEnd =>
        blob
    }
    .rechunk(1000)
    .mapChunks(files => Chunk(files.map(_.name)))
    .rechunk(1)

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
        .mapZIO(file => fileToStream(file, changeSetSchema))
    }
