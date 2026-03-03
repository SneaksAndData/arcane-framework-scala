package com.sneaksanddata.arcane.framework
package services.blobsource.readers.listing

import logging.ZIOLogAnnotations.zlog
import models.batches.BlobBatchCommons
import models.schemas.{*, given}
import models.settings.sources.blob.ParquetBlobSourceSettings
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

import java.util.Base64

class BlobListingParquetSource[PathType <: BlobPath](
    sourcePath: PathType,
    reader: BlobStorageReader[PathType],
    tempStoragePath: String,
    primaryKeys: Seq[String],
    useNameMapping: Boolean,
    sourceSchema: Option[String]
) extends BlobListingSource[PathType](sourcePath, reader, primaryKeys)
    with SchemaProvider[ArcaneSchema]:

  override def getSchema: Task[SchemaType] = for
    preconfiguredSchema <- ZIO.when(sourceSchema.isDefined) {
      for
        schemaBytes <- ZIO.attempt(Base64.getDecoder.decode(sourceSchema.get))
        scanner     <- ZIO.attempt(ParquetScanner(schemaBytes, useNameMapping))
        schema      <- scanner.getIcebergSchema.map(implicitly)
      yield schema
    }
    runtimeSchema <- preconfiguredSchema match
      case Some(schema) => ZIO.succeed(schema)
      case None =>
        for
          _ <- zlog(
            "No sourceSchema provided for the stream, will try to infer from source data. It is advised to avoid reliance on automatic schema resolution, as this can cause data corruption or stream failure if source is empty"
          )
          maybeFilePath <- reader.downloadRandomBlob(sourcePath, tempStoragePath)
          schema <- maybeFilePath match
            case Some(filePath) =>
              ZIO.attempt(ParquetScanner(filePath, useNameMapping)).flatMap(_.getIcebergSchema.map(implicitly))
            case None =>
              ZIO.fail(
                new RuntimeException(
                  s"Unable to locate schema for $sourcePath - stream will terminate. Please check if bucket is not empty when stream starts, or provide `sourceSchema` value to avoid automatic inference"
                )
              )
        yield schema
  yield runtimeSchema ++ Seq(BlobBatchCommons.indexedVersionField(runtimeSchema.mergeKey match {
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
    sourceFile <- reader
      .streamPrefixes(sourcePath)
      .filter(_.createdOn.map(BlobSourceWatermark.fromEpochSecond).getOrElse(BlobSourceWatermark.epoch) >= startFrom)
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
      useNameMapping: Boolean,
      sourceSchema: Option[String]
  ): BlobListingParquetSource[S3StoragePath] =
    new BlobListingParquetSource[S3StoragePath](
      sourcePath,
      s3Reader,
      tempPath,
      primaryKeys,
      useNameMapping,
      sourceSchema
    )

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
      sourceSettings.useNameMapping,
      sourceSettings.sourceSchema
    )
  }
