package com.sneaksanddata.arcane.framework
package services.blobsource.readers.listing

import services.blobsource.readers.BlobSourceReader
import services.blobsource.versioning.BlobSourceWatermark
import services.storage.base.BlobStorageReader
import services.storage.models.base.BlobPath

import zio.stream.ZSink
import zio.{Task, ZIO}

import java.security.MessageDigest
import java.time.{Duration, Instant, OffsetDateTime, ZoneOffset}

abstract class BlobListingSource[PathType <: BlobPath](
    sourcePath: PathType,
    reader: BlobStorageReader[PathType],
    primaryKeys: Seq[String]
) extends BlobSourceReader:

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
