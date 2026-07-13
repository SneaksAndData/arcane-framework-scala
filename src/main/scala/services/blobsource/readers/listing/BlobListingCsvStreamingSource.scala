package com.sneaksanddata.arcane.framework
package services.blobsource.readers.listing

import models.schemas.{ArcaneSchema, DataRow}
import services.blobsource.versioning.BlobSourceWatermark
import services.storage.base.BlobStorageReader
import services.storage.models.base.{BlobPath, StoredBlob}
import services.streaming.base.StructuredZStream

import zio.Task
import zio.stream.ZStream

class BlobListingCsvStreamingSource[PathType <: BlobPath](
    sourcePath: PathType,
    reader: BlobStorageReader[PathType],
    schema: ArcaneSchema,
    primaryKeys: Seq[String]
) extends BlobListingStreamingSource[PathType](sourcePath, reader, primaryKeys):

  override def getSchema: Task[SchemaType] = ???

  /** Gets an empty schema.
    *
    * @return
    *   An empty schema.
    */
  override def empty: SchemaType = ArcaneSchema.empty()

  override def getChanges(startFrom: BlobSourceWatermark): ZStream[Any, Throwable, StructuredZStream] = ???

  override def getLatestVersion: Task[BlobSourceWatermark] = ???

  override def hasChanges(previousVersion: BlobSourceWatermark): Task[Boolean] = ???

  /** Creates a structured stream for a provided file address
    */
  override def fileToStream(sourceFile: StoredBlob): Task[(ZStream[Any, Throwable, DataRow], ArcaneSchema)] = ???

  override def filesToStream(sourceFiles: Seq[StoredBlob]): Task[(ZStream[Any, Throwable, DataRow], ArcaneSchema)] = ???
