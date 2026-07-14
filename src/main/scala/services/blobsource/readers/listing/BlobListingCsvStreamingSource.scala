package com.sneaksanddata.arcane.framework
package services.blobsource.readers.listing

import models.schemas.{ArcaneSchema, DataRow}
import services.blobsource.versioning.BlobSourceWatermark
import services.naming.NameGenerator
import services.storage.base.{BlobStorageReader, BlobStorageWriter}
import services.storage.models.base.{BlobPath, StoredBlob}
import services.streaming.base.StructuredZStream

import zio.Task
import zio.stream.ZStream

class BlobListingCsvStreamingSource[PathType <: BlobPath](
    sourcePath: PathType,
    shardStoragePath: PathType,
    storageClient: BlobStorageReader[PathType] & BlobStorageWriter[PathType],
    nameGenerator: NameGenerator,
    schema: ArcaneSchema,
    primaryKeys: Seq[String]
) extends BlobListingStreamingSource[PathType](sourcePath, shardStoragePath, storageClient, nameGenerator, primaryKeys):

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

  override def filesToStream(
      sourceFiles: Seq[StoredBlob],
      schema: ArcaneSchema
  ): Task[(ZStream[Any, Throwable, DataRow], ArcaneSchema)] = ???
