package com.sneaksanddata.arcane.framework
package services.blobsource.base

import models.schemas.{ArcaneSchema, DataRow, given_CanAdd_ArcaneSchema}

import com.sneaksanddata.arcane.framework.services.base.SchemaProvider
import com.sneaksanddata.arcane.framework.services.iceberg.interop.ParquetScanner
import com.sneaksanddata.arcane.framework.services.storage.base.BlobStorageReader
import com.sneaksanddata.arcane.framework.services.storage.models.base.BlobPath
import com.sneaksanddata.arcane.framework.services.storage.models.s3.S3StoragePath
import zio.{Task, ZIO}
import zio.stream.ZStream

class ParquetBlobSourceReader[PathType <: BlobPath](sourcePath: PathType, reader: BlobStorageReader[PathType], temptStoragePath: String) extends SchemaProvider[ArcaneSchema]:
 
  override def getSchema: Task[SchemaType] = for
    filePath <- reader.downloadRandomBlob(sourcePath, temptStoragePath)
    scanner <- ZIO.attempt(ParquetScanner(filePath))
    schema <- scanner.getIcebergSchema.map(implicitly)
  yield schema

  /** Gets an empty schema.
   *
   * @return
   * An empty schema.
   */
  override def empty: SchemaType = ArcaneSchema.empty()
  
  def getChanges: ZStream[Any, Throwable, DataRow] = ??? 
