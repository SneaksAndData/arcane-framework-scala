package com.sneaksanddata.arcane.framework
package services.blobsource.readers.listing

import models.schemas.{ArcaneSchema, DataRow, given_CanAdd_ArcaneSchema}
import services.base.SchemaProvider
import services.blobsource.readers.BlobSourceReader
import services.storage.base.BlobStorageReader
import services.storage.models.base.BlobPath
import services.storage.models.s3.S3StoragePath

import com.sneaksanddata.arcane.framework.services.blobsource.BlobSourceVersion
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

  override def getChanges(startFrom: BlobSourceVersion): ZStream[Any, Throwable, OutputRow] = ???

  override def getLatestVersion: Task[BlobSourceVersion] = ???

  override def getStartFrom(lookBackInterval: Duration): Task[BlobSourceVersion] = ???

  override def hasChanges(previousVersion: BlobSourceVersion): Task[Boolean] = ???
