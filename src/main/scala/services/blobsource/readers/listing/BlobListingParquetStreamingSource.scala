package com.sneaksanddata.arcane.framework
package services.blobsource.readers.listing

import exceptions.FatalStreamFailException
import logging.ZIOLogAnnotations.zlog
import models.app.PluginStreamContext
import models.batches.BlobBatchCommons
import models.schemas.{*, given}
import models.settings.sources.blob.ParquetBlobSourceSettings
import models.settings.sources.{DataRowModification, DataRowSchemaVersion}
import services.iceberg.interop.ParquetScanner
import services.iceberg.{given_Conversion_Schema_Seq, inferMergeKeyIndex}
import services.naming.NameGenerator
import services.storage.base.{BlobStorageReader, BlobStorageWriter}
import services.storage.models.base.{BlobPath, StoredBlob}
import services.storage.models.s3.S3StoragePath
import services.storage.services.s3.S3BlobStorageService
import services.streaming.base.StructuredZStream

import org.apache.iceberg.Schema
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
    sourceSchema: Option[String],
    modifications: Seq[DataRowModification] = Seq.empty,
    dataRowSchemaVersion: DataRowSchemaVersion = DataRowSchemaVersion.V0
) extends BlobListingStreamingSource[PathType](
      sourcePath,
      shardStoragePath,
      storageClient,
      nameGenerator,
      primaryKeys,
      tempStoragePath,
      modifications,
      dataRowSchemaVersion
    ):

  override protected def getSourceSchema: Task[SchemaType] = for
    preconfiguredSchema <- ZIO.when(sourceSchema.isDefined) {
      for
        schemaBytes <- ZIO.attempt(Base64.getDecoder.decode(sourceSchema.get))
        scanner     <- ZIO.attempt(ParquetScanner(schemaBytes, useNameMapping))
        schema      <- scanner.getIcebergSchema
      yield schema
    }
    icebergSchema <- preconfiguredSchema match
      case Some(schema) => ZIO.succeed(schema)
      case None =>
        for
          _ <- zlog(
            "No sourceSchema provided for the stream, will try to infer from source data. It is advised to avoid reliance on automatic schema resolution, as this can cause data corruption or stream failure if source is empty"
          )
          maybeFilePath <- storageClient.downloadRandomBlob(sourcePath, tempStoragePath)
          schema <- maybeFilePath match
            case Some(filePath) =>
              ZIO.attempt(ParquetScanner(filePath, useNameMapping)).flatMap(_.getIcebergSchema)
            case None =>
              ZIO.fail(
                FatalStreamFailException(
                  s"Unable to locate schema for $sourcePath - stream will terminate. Please check if bucket is not empty when stream starts, or provide `sourceSchema` value to avoid automatic inference"
                )
              )
        yield schema

    originalFields: Seq[IndexedField] = summon[
      Conversion[org.apache.iceberg.Schema, Seq[IndexedField]]
    ].apply(icebergSchema)
    originalSchema = ArcaneSchema(originalFields)
    nextFieldId    = inferMergeKeyIndex(icebergSchema.columns().getLast)
  yield
    if dataRowSchemaVersion.usesCommonMergeKey then
      originalSchema ++ Seq(
        BlobBatchCommons.indexedVersionField(nextFieldId)
      )
    else
      originalSchema ++ Seq(
        IndexedMergeKeyField(nextFieldId),
        BlobBatchCommons.indexedVersionField(nextFieldId + 1)
      )

  /** Gets an empty schema.
    *
    * @return
    *   An empty schema.
    */
  override def empty: SchemaType = ArcaneSchema.empty()

  def fileToStream(sourceFile: StoredBlob, schema: ArcaneSchema): Task[StructuredZStream] = for
    downloadedFilePath <- downloadSourceFile(sourceFile)
    scanner            <- ZIO.attempt(ParquetScanner(downloadedFilePath, useNameMapping))
  yield (
    scanner.getRows.map(
      BlobBatchCommons.enrichBatchRow(
        _,
        sourceFile.createdOn.getOrElse(0),
        primaryKeys,
        mergeKeyHasher(),
        !dataRowSchemaVersion.usesCommonMergeKey
      )
    ),
    schema
  )

  override def filesToStream(
      sourceFiles: Seq[StoredBlob],
      schema: ArcaneSchema
  ): Task[(ZStream[Any, Throwable, DataRow], ArcaneSchema)] =
    ZIO.attempt(
      ZStream
        .fromIterable(sourceFiles)
        .flatMapPar(parallelism) { sourceFile =>
          ZStream
            .fromZIO {
              for
                filePath <- downloadSourceFile(sourceFile)
                scanner  <- ZIO.attempt(ParquetScanner(filePath, useNameMapping))
              yield scanner
            }
            .flatMap(
              _.getRows.map(
                BlobBatchCommons.enrichBatchRow(
                  _,
                  sourceFile.createdOn.getOrElse(0),
                  primaryKeys,
                  mergeKeyHasher(),
                  !dataRowSchemaVersion.usesCommonMergeKey
                )
              )
            )
        },
      schema
    )

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
        sourceSchema = sourceSettings.sourceSchema,
        modifications = context.source.modifications.modifications,
        dataRowSchemaVersion = context.source.dataRowSchemaVersion
      )
    }
