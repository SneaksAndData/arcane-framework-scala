package com.sneaksanddata.arcane.framework
package services.blobsource.base

import models.schemas.{ArcaneSchema, DataRow, given_CanAdd_ArcaneSchema}
import services.base.SchemaProvider
import services.storage.base.BlobStorageReader
import services.storage.models.base.BlobPath
import services.storage.models.s3.S3StoragePath

import zio.stream.ZStream
import zio.{Task, ZIO}

class JsonBlobSourceReader[PathType <: BlobPath](blobPath: PathType, reader: BlobStorageReader[PathType], schema: ArcaneSchema) extends SchemaProvider[ArcaneSchema]:
 
  override def getSchema: Task[SchemaType] = ???

  /** Gets an empty schema.
   *
   * @return
   * An empty schema.
   */
  override def empty: SchemaType = ArcaneSchema.empty()
  
  def getChanges: ZStream[Any, Throwable, DataRow] = ??? 
