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
import services.naming.NameGenerator
import services.storage.base.{BlobStorageReader, BlobStorageWriter}
import services.storage.models.base.{BlobPath, StoredBlob}
import services.storage.models.s3.S3StoragePath
import services.storage.services.s3.S3BlobStorageService
import services.streaming.base.StructuredZStream

import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

import java.util.Base64

class BlobListingParquetStreamingSource[PathType <: BlobPath](
    sourcePath: PathType,
    shardStoragePath: PathType,
    storageClient: BlobStorageReader[PathType] & BlobStorageWriter[PathType],
    nameGenerator: NameGenerator,
    tempStoragePath: String,
    primaryKeys: Seq[String],
    useNameMapping: Boolean,
    sourceSchema: Option[String]
) extends BlobListingStreamingSource[PathType](sourcePath, shardStoragePath, storageClient, nameGenerator, primaryKeys):

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
          maybeFilePath <- storageClient.downloadRandomBlob(sourcePath, tempStoragePath)
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
    storageClient
      .downloadBlob(s"${sourcePath.protocol}://${sourceFile.name}", tempStoragePath)
      .map(filePath => ParquetScanner(filePath, useNameMapping))
      .flatMap { scanner =>
        getSchema
          .map(schema =>
            (
              scanner.getRows.map(
                BlobBatchCommons.enrichBatchRow(_, sourceFile.createdOn.getOrElse(0), primaryKeys, mergeKeyHasher())
              ),
              schema
            )
          )
      }

  override def filesToStream(sourceFiles: Seq[StoredBlob]): Task[(ZStream[Any, Throwable, DataRow], ArcaneSchema)] =
    getSchema.map { schema =>
      val stream = ZStream
        .fromIterable(sourceFiles)
        .flatMap { sourceFile =>
          ZStream
            .fromZIO {
              for
                filePath <- storageClient.downloadBlob(s"${sourcePath.protocol}://${sourceFile.name}", tempStoragePath)
                scanner  <- ZIO.attempt(ParquetScanner(filePath, useNameMapping))
              yield scanner
            }
            .flatMap(
              _.getRows.map(
                BlobBatchCommons.enrichBatchRow(_, sourceFile.createdOn.getOrElse(0), primaryKeys, mergeKeyHasher())
              )
            )
        }
      (stream, schema)
    }

  override def getChanges(startFrom: BlobSourceWatermark): ZStream[Any, Throwable, StructuredZStream] = storageClient
    .streamPrefixes(sourcePath)
    .filter(_.createdOn.map(BlobSourceWatermark.fromEpochSecond).getOrElse(BlobSourceWatermark.epoch) >= startFrom)
    .mapZIO(fileToStream)

object BlobListingParquetStreamingSource:
  private type SettingsExtractor = PluginStreamContext => ParquetBlobSourceSettings

  /** Default layer is S3. Provide your own layer (Azure etc.) through plugin override if needed
    */
  def getS3Layer(
      extractor: SettingsExtractor
  ): ZLayer[S3BlobStorageService & NameGenerator & PluginStreamContext, Throwable, BlobListingParquetStreamingSource[
    S3StoragePath
  ]] =
    ZLayer {
      for
        context        <- ZIO.service[PluginStreamContext]
        storageService <- ZIO.service[S3BlobStorageService]
        nameGenerator  <- ZIO.service[NameGenerator]
        sourceSettings <- ZIO.attempt(extractor(context))
        sourcePath <- ZIO.getOrFailWith(new IllegalArgumentException("Invalid S3 source path provided"))(
          S3StoragePath(sourceSettings.sourcePath).toOption
        )
        shardStoragePath <- ZIO.getOrFailWith(new IllegalArgumentException("Invalid S3 shard storage path provided"))(
          S3StoragePath(sourceSettings.shardStoragePath).toOption
        )
      yield new BlobListingParquetStreamingSource(
        sourcePath = sourcePath,
        shardStoragePath = shardStoragePath,
        storageClient = storageService,
        nameGenerator = nameGenerator,
        tempStoragePath = sourceSettings.tempStoragePath,
        primaryKeys = sourceSettings.primaryKeys,
        useNameMapping = sourceSettings.useNameMapping,
        sourceSchema = sourceSettings.sourceSchema
      )
    }
