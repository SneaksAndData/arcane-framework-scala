package com.sneaksanddata.arcane.framework
package services.blobsource.readers

import services.base.StreamingSource
import services.blobsource.versioning.BlobSourceWatermark
import services.storage.models.base.StoredBlob
import services.streaming.base.StructuredZStream

import zio.{Chunk, Task, ZIO}
import zio.stream.{ZPipeline, ZStream}

import java.nio.charset.StandardCharsets
import java.util.Base64

/** Base trait for all blob source readers
  */
trait BlobStreamingSource extends StreamingSource:

  final override type ShardMetadata = Seq[String]
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

  /** Creates a structured stream for a list of provided files addresses
    */
  def filesToStream(sourceFiles: Seq[StoredBlob]): Task[StructuredZStream]

  /** Extracts file names from a shard name
    */
  def unpackShard(shardName: String): Task[Seq[String]] = for
    decoded <- ZIO.succeed(Base64.getUrlDecoder.decode(shardName.getBytes(StandardCharsets.ISO_8859_1)))
    uncompressed <- ZStream
      .fromChunk(Chunk.fromIterable(decoded))
      .via(ZPipeline.gunzip())
      .via(ZPipeline.utf8Decode)
      .runCollect
      .map(_.mkString)
  yield uncompressed.split("\n").toSeq

  /** Packs file names into a single string which serves as a shard name
    */
  def packShard(sourceFiles: Seq[String]): Task[String] = for
    compressed <- ZStream.fromIterable(sourceFiles.mkString("\n").getBytes("UTF-8")).via(ZPipeline.gzip()).runCollect
    encoded    <- ZIO.succeed(Base64.getUrlEncoder.withoutPadding.encodeToString(compressed.toArray))
  yield encoded

  /** Creates a StoredBlob model from a provided file address
    */
  def fileToBlob(sourceFile: String): Task[StoredBlob]

  override def getShards(
      rangeStart: BlobSourceWatermark,
      rangeEnd: BlobSourceWatermark
  ): ZStream[Any, Throwable, Seq[String]]
