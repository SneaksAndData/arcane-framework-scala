package com.sneaksanddata.arcane.framework
package services.blobsource.readers.listing

import models.batches.BlobBatchCommons
import models.schemas.{*, given}
import models.settings.blob.ParquetBlobSourceSettings
import services.base.SchemaProvider
import services.blobsource.versioning.BlobSourceWatermark
import services.iceberg.given_Conversion_Schema_ArcaneSchema
import services.iceberg.interop.ParquetScanner
import services.storage.base.BlobStorageReader
import services.storage.models.base.BlobPath
import services.storage.models.s3.S3StoragePath
import services.storage.services.s3.S3BlobStorageReader

import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

class BlobListingParquetSource[PathType <: BlobPath](
    sourcePath: PathType,
    reader: BlobStorageReader[PathType],
    tempStoragePath: String,
    primaryKeys: Seq[String],
    useNameMapping: Boolean
) extends BlobListingSource[PathType](sourcePath, reader, primaryKeys)
    with SchemaProvider[ArcaneSchema]:

  override def getSchema: Task[SchemaType] = for
    filePath <- reader.downloadRandomBlob(sourcePath, tempStoragePath)
    scanner  <- ZIO.attempt(ParquetScanner(filePath, useNameMapping))
    schema   <- scanner.getIcebergSchema.map(implicitly)
  yield schema ++ Seq(BlobBatchCommons.indexedVersionField(schema.mergeKey match {
    case IndexedMergeKeyField(fieldId) => fieldId + 1
    case _ => throw new RuntimeException("Unsupported schema: parquet source supplied a non-indexed merge key")
  }))

  /** Gets an empty schema.
    *
    * @return
    *   An empty schema.
    */
  override def empty: SchemaType = ArcaneSchema.empty()

  override def getChanges(startFrom: BlobSourceWatermark): ZStream[Any, Throwable, OutputRow] = for
    sourceFile <- reader.streamPrefixes(sourcePath).filter(_.createdOn.getOrElse(0L) >= startFrom.version.toLong)
    downloadedFile <- ZStream.fromZIO(
      reader.downloadBlob(s"${sourcePath.protocol}://${sourceFile.name}", tempStoragePath)
    )
    scanner <- ZStream.fromZIO(ZIO.attempt(ParquetScanner(downloadedFile, useNameMapping)))
    row <- scanner.getRows.map(
      BlobBatchCommons.enrichBatchRow(_, sourceFile.createdOn.getOrElse(0), primaryKeys, mergeKeyHasher)
    )
  yield row

object BlobListingParquetSource:
  def apply(
      sourcePath: S3StoragePath,
      s3Reader: S3BlobStorageReader,
      tempPath: String,
      primaryKeys: Seq[String],
      useNameMapping: Boolean
  ): BlobListingParquetSource[S3StoragePath] =
    new BlobListingParquetSource[S3StoragePath](sourcePath, s3Reader, tempPath, primaryKeys, useNameMapping)

  /** Default layer is S3. Provide your own layer (Azure etc.) through plugin override if needed
    */
  val layer: ZLayer[ParquetBlobSourceSettings & S3BlobStorageReader, IllegalArgumentException, BlobListingParquetSource[
    S3StoragePath
  ]] = ZLayer {
    for
      blobReader     <- ZIO.service[S3BlobStorageReader]
      sourceSettings <- ZIO.service[ParquetBlobSourceSettings]
      sourcePath <- ZIO.getOrFailWith(new IllegalArgumentException("Invalid S3 path provided"))(
        S3StoragePath(sourceSettings.sourcePath).toOption
      )
    yield BlobListingParquetSource(
      sourcePath,
      blobReader,
      sourceSettings.tempStoragePath,
      sourceSettings.primaryKeys,
      sourceSettings.useNameMapping
    )
  }
