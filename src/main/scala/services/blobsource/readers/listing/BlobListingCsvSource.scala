package com.sneaksanddata.arcane.framework
package services.blobsource.readers.listing

import models.schemas.{ArcaneSchema, DataRow, given_CanAdd_ArcaneSchema}
import services.base.SchemaProvider
import services.blobsource.readers.BlobSourceReader
import services.storage.base.BlobStorageReader
import services.storage.models.base.BlobPath
import services.storage.models.s3.S3StoragePath

import zio.stream.ZStream
import zio.{Task, ZIO}

import java.time.Duration

class BlobListingCsvSource[PathType <: BlobPath](
    blobPath: PathType,
    reader: BlobStorageReader[PathType],
    schema: ArcaneSchema,
    primaryKeys: Seq[String]
) extends BlobSourceReader
    with SchemaProvider[ArcaneSchema]:

  override type OutputRow = DataRow

  override def getSchema: Task[SchemaType] = ???

  /** Gets an empty schema.
    *
    * @return
    *   An empty schema.
    */
  override def empty: SchemaType = ArcaneSchema.empty()

  override def getChanges(startFrom: Long): ZStream[Any, Throwable, (OutputRow, Long)] = ???

  override def getStartFrom(lookBackInterval: Duration): Task[Long] = ZIO.succeed(0)

  override def getLatestVersion: Task[Long] = ZIO.succeed(0)
