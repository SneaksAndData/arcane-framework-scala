package com.sneaksanddata.arcane.framework
package services.blobsource.readers

import services.base.StreamingSource
import services.blobsource.versioning.BlobSourceWatermark
import services.storage.models.base.StoredBlob
import services.streaming.base.StructuredZStream

import zio.Task
import zio.stream.ZStream

/** Base trait for all blob source readers
  */
trait BlobSourceReader extends StreamingSource:

  final override type ShardMetadata = StoredBlob
  final override type WatermarkType = BlobSourceWatermark

  /** Change stream for this reader. If startFrom == 0, should behave like a backfill.
    * @param startFrom
    *   Time (Unix) to emit changes from for next iteration
    * @return
    */
  def getChanges(startFrom: BlobSourceWatermark): ZStream[Any, Throwable, StructuredZStream]

  /** Latest file by creation data in the blob source
    */
  def getLatestVersion: Task[BlobSourceWatermark]

  /** Return true if a blob source has files with creation date > previousVersion
    */
  def hasChanges(previousVersion: BlobSourceWatermark): Task[Boolean]

  /** Creates a structured stream for a provided file address
    */
  def fileToStream(sourceFile: StoredBlob): Task[StructuredZStream]

  /** Creates a StoredBlob model from a provided file address
    */
  def fileToBlob(sourceFile: String): Task[StoredBlob]

  override def getShards(
      rangeStart: BlobSourceWatermark,
      rangeEnd: BlobSourceWatermark
  ): ZStream[Any, Throwable, StoredBlob]
