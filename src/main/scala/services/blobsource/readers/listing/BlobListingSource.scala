package com.sneaksanddata.arcane.framework
package services.blobsource.readers.listing

import services.blobsource.BlobSourceVersion
import services.blobsource.readers.BlobSourceReader
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

  override def getLatestVersion: Task[BlobSourceVersion] = reader
    .streamPrefixes(sourcePath)
    .map(_.createdOn.getOrElse(0L))
    .run(ZSink.foldLeft(0L)((e, agg) => if (e > agg) e else agg))
    .map(createdOn => BlobSourceVersion(versionNumber = createdOn.toString, waterMarkTime = OffsetDateTime.ofInstant(Instant.ofEpochMilli(createdOn), ZoneOffset.UTC)))

  // Listing readers do not support versioned streams, since they do not keep track of which file has been or not been processed
  // thus they always act like they lookback until beginning of time
  override def getStartFrom(lookBackInterval: Duration): Task[BlobSourceVersion] =  
    for
     version <- ZIO.succeed(
        OffsetDateTime
          .now()
          .minus(lookBackInterval)
          .toInstant
          .toEpochMilli / 1000
      )
    yield BlobSourceVersion(
      versionNumber = version.toString, OffsetDateTime.ofInstant(Instant.ofEpochMilli(version), ZoneOffset.UTC)
    )

  // due to the fact that this is always called by StreamingDataProvider after comparing versions
  // and the fact that versions are file creation dates, we can safely assume that IF this method is called, it will return TRUE. Hence no need to double list files
  override def hasChanges(previousVersion: BlobSourceVersion): Task[Boolean] = ZIO.succeed(true)
  
