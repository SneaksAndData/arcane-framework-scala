package com.sneaksanddata.arcane.framework
package services.blobsource.readers.listing

import models.schemas.{ArcaneSchema, DataRow}
import models.settings.FieldSelectionRuleSettings
import models.settings.sources.{DataRowModification, DataRowSchemaVersion}
import services.blobsource.versioning.BlobSourceWatermark
import services.naming.NameGenerator
import services.storage.base.{BlobStorageReader, BlobStorageWriter}
import services.storage.models.base.{BlobPath, StoredBlob}

import zio.Task
import zio.stream.ZStream

class BlobListingCsvStreamingSource[PathType <: BlobPath](
    sourcePath: PathType,
    shardStoragePath: PathType,
    storageClient: BlobStorageReader[PathType] & BlobStorageWriter[PathType],
    nameGenerator: NameGenerator,
    schema: ArcaneSchema,
    primaryKeys: Seq[String],
    tempStoragePath: String,
    fieldSelector: FieldSelectionRuleSettings,
    modifications: Seq[DataRowModification] = Seq.empty,
    dataRowSchemaVersion: DataRowSchemaVersion = DataRowSchemaVersion.V0
) extends BlobListingStreamingSource[PathType](
      sourcePath,
      shardStoragePath,
      storageClient,
      nameGenerator,
      primaryKeys,
      tempStoragePath,
      fieldSelector,
      modifications,
      dataRowSchemaVersion
    ):

  def this(
      sourcePath: PathType,
      shardStoragePath: PathType,
      storageClient: BlobStorageReader[PathType] & BlobStorageWriter[PathType],
      nameGenerator: NameGenerator,
      schema: ArcaneSchema,
      primaryKeys: Seq[String],
      tempStoragePath: String,
      fieldSelector: FieldSelectionRuleSettings
  ) = this(
    sourcePath,
    shardStoragePath,
    storageClient,
    nameGenerator,
    schema,
    primaryKeys,
    tempStoragePath,
    fieldSelector,
    Seq.empty,
    DataRowSchemaVersion.V0
  )

  override protected def getSourceSchema: Task[SchemaType] = ???

  /** Gets an empty schema.
    *
    * @return
    *   An empty schema.
    */
  override def empty: SchemaType = ArcaneSchema.empty()

  override def hasChanges(previousVersion: BlobSourceWatermark): Task[Boolean] = ???

  /** Creates a structured stream for a provided file address
    */
  override def fileToStream(
      sourceFile: StoredBlob,
      schema: ArcaneSchema
  ): Task[(ZStream[Any, Throwable, DataRow], ArcaneSchema)] = ???

  override def filesToStream(
      sourceFiles: Seq[StoredBlob],
      schema: ArcaneSchema
  ): Task[(ZStream[Any, Throwable, DataRow], ArcaneSchema)] = ???
