//package com.sneaksanddata.arcane.framework
//package services.blobsource.base
//
//import models.schemas.ArcaneSchema
//
//import com.sneaksanddata.arcane.framework.models.schemas.given_CanAdd_ArcaneSchema
//import com.sneaksanddata.arcane.framework.services.base.SchemaProvider
//import com.sneaksanddata.arcane.framework.services.storage.base.BlobStorageReader
//import com.sneaksanddata.arcane.framework.services.storage.models.base.BlobPath
//import zio.Task
//
//final class BlobSourceReader[PathType <: BlobPath](reader: BlobStorageReader[PathType]) extends SchemaProvider[ArcaneSchema]:
//  override def getSchema: Task[SchemaType] = reader.readBlobContent()
//
//  /** Gets an empty schema.
//   *
//   * @return
//   * An empty schema.
//   */
//  override def empty: SchemaType = ???
