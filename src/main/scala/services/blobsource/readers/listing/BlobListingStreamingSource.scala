package com.sneaksanddata.arcane.framework
package services.blobsource.readers.listing

import models.schemas.given_CanAdd_ArcaneSchema
import services.blobsource.readers.BlobStreamingSource
import services.blobsource.versioning.BlobSourceWatermark
import services.storage.base.BlobStorageReader
import services.storage.models.base.{BlobPath, StoredBlob}

import zio.stream.{ZPipeline, ZSink, ZStream}
import zio.{Chunk, Task, ZIO}

import java.security.MessageDigest
import java.util.{Base64, UUID}

abstract class BlobListingStreamingSource[PathType <: BlobPath](
    sourcePath: PathType,
    reader: BlobStorageReader[PathType],
    primaryKeys: Seq[String]
) extends BlobStreamingSource:

  override def fileToBlob(sourceFile: String): Task[StoredBlob] = reader.blobMetadata(sourceFile)

  final override def deleteShards(prefix: String): Task[Unit] = ZIO.unit

  /** SHA-256 hasher.
    */
  protected def mergeKeyHasher(): MessageDigest = MessageDigest.getInstance("SHA-256")

  override def getLatestVersion: Task[BlobSourceWatermark] = reader
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
  ): ZStream[Any, Throwable, Seq[String]] = reader
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
