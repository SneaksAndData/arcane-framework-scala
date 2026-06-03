package com.sneaksanddata.arcane.framework
package services.blobsource.readers.listing

import models.schemas.given_CanAdd_ArcaneSchema
import services.blobsource.readers.BlobSourceReader
import services.blobsource.versioning.BlobSourceWatermark
import services.storage.base.BlobStorageReader
import services.storage.models.base.{BlobPath, StoredBlob}

import zio.stream.{ZSink, ZStream}
import zio.{Task, ZIO}

import java.security.MessageDigest

abstract class BlobListingSource[PathType <: BlobPath](
    sourcePath: PathType,
    reader: BlobStorageReader[PathType],
    primaryKeys: Seq[String]
) extends BlobSourceReader:
  
  override def fileToBlob(sourceFile: String): Task[StoredBlob] = reader.blobMetadata(sourceFile)

  final override def deleteShards(prefix: String): Task[Unit] = ZIO.unit

  /** SHA-256 hasher. Note that this is NOT thread safe and must not be created outside the BlobListingSource
    */
  protected val mergeKeyHasher: MessageDigest = MessageDigest.getInstance("SHA-256")

  override def getLatestVersion: Task[BlobSourceWatermark] = reader
    .streamPrefixes(sourcePath)
    .map(_.createdOn.getOrElse(0L))
    .run(ZSink.foldLeft(0L)((e, agg) => if (e > agg) e else agg))
    .map(BlobSourceWatermark.fromEpochSecond)

  // due to the fact that this is always called by StreamingDataProvider after comparing versions
  // and the fact that versions are file creation dates, we can safely assume that IF this method is called, it will return TRUE. Hence no need to double list files
  override def hasChanges(previousVersion: BlobSourceWatermark): Task[Boolean] = ZIO.succeed(true)

  final override def getShards(backfillId: String, rangeStart: BlobSourceWatermark, rangeEnd: BlobSourceWatermark): ZStream[Any, Throwable, StoredBlob] = reader
    .streamPrefixes(sourcePath)
    .collect {
      case blob if blob.createdOn.map(BlobSourceWatermark.fromEpochSecond).getOrElse(BlobSourceWatermark.epoch) >= rangeStart
        && blob.createdOn.map(BlobSourceWatermark.fromEpochSecond).getOrElse(BlobSourceWatermark.epoch) <= rangeEnd => blob
    }
