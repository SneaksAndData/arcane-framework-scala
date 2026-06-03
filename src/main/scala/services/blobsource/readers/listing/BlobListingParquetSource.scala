package com.sneaksanddata.arcane.framework
package services.blobsource.readers.listing

import logging.ZIOLogAnnotations.zlog
import models.app.PluginStreamContext
import models.batches.BlobBatchCommons
import models.schemas.{*, given}
import models.settings.sources.blob.ParquetBlobSourceSettings
import services.blobsource.versioning.BlobSourceWatermark
import services.iceberg.given_Conversion_Schema_ArcaneSchema
import services.iceberg.interop.ParquetScanner
import services.storage.base.BlobStorageReader
import services.storage.models.base.{BlobPath, StoredBlob}
import services.storage.models.s3.S3StoragePath
import services.storage.services.s3.S3BlobStorageReader
import services.streaming.base.StructuredZStream

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
) extends BlobListingSource[PathType](sourcePath, reader, primaryKeys):

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

  def fileToStream(sourceFile: StoredBlob): Task[StructuredZStream] =
    reader
      .downloadBlob(s"${sourcePath.protocol}://${sourceFile.name}", tempStoragePath)
      .map(filePath => ParquetScanner(filePath, useNameMapping))
      .flatMap { scanner =>
        scanner.getIcebergSchema
          .map(implicitly)
          .map(schema =>
            (
              scanner.getRows.map(
                BlobBatchCommons.enrichBatchRow(_, sourceFile.createdOn.getOrElse(0), primaryKeys, mergeKeyHasher)
              ),
              schema
            )
          )
      }

  override def getChanges(startFrom: BlobSourceWatermark): ZStream[Any, Throwable, StructuredZStream] = reader
    .streamPrefixes(sourcePath)
    .filter(_.createdOn.map(BlobSourceWatermark.fromEpochSecond).getOrElse(BlobSourceWatermark.epoch) >= startFrom)
    .mapZIO(fileToStream)

//  override def getShards(backfillId: String, rangeStart: BlobSourceWatermark, rangeEnd: BlobSourceWatermark): ZStream[Any, Throwable, StoredBlob] = reader
//    .streamPrefixes(sourcePath)
//    .collect {
//      case blob if blob.createdOn.map(BlobSourceWatermark.fromEpochSecond).getOrElse(BlobSourceWatermark.epoch) >= rangeStart
//       && blob.createdOn.map(BlobSourceWatermark.fromEpochSecond).getOrElse(BlobSourceWatermark.epoch) <= rangeEnd => blob
//    }

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

  private type SettingsExtractor = PluginStreamContext => ParquetBlobSourceSettings

  /** Default layer is S3. Provide your own layer (Azure etc.) through plugin override if needed
    */
  def getLayer(
      extractor: SettingsExtractor
  ): ZLayer[S3BlobStorageReader & PluginStreamContext, Throwable, BlobListingParquetSource[S3StoragePath]] = ZLayer {
    for
      context        <- ZIO.service[PluginStreamContext]
      blobReader     <- ZIO.service[S3BlobStorageReader]
      sourceSettings <- ZIO.attempt(extractor(context))
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
