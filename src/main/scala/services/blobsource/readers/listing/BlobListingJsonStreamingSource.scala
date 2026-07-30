package com.sneaksanddata.arcane.framework
package services.blobsource.readers.listing

import models.app.PluginStreamContext
import models.batches.BlobBatchCommons
import models.schemas.{ArcaneSchema, DataRow}
import models.settings.sources.blob.JsonBlobSourceSettings
import services.blobsource.versioning.BlobSourceWatermark
import services.iceberg.given_Conversion_AvroSchema_ArcaneSchema
import services.iceberg.interop.JsonScanner
import services.naming.NameGenerator
import services.storage.base.{BlobStorageReader, BlobStorageWriter}
import services.storage.models.base.{BlobPath, StoredBlob}
import services.storage.models.s3.S3StoragePath
import services.storage.services.s3.S3BlobStorageService
import services.streaming.base.StructuredZStream

import org.apache.avro.Schema as AvroSchema
import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

class BlobListingJsonStreamingSource[PathType <: BlobPath](
    sourcePath: PathType,
    shardStoragePath: PathType,
    storageClient: BlobStorageReader[PathType] & BlobStorageWriter[PathType],
    nameGenerator: NameGenerator,
    tempStoragePath: String,
    primaryKeys: Seq[String],
    avroSchemaString: String,
    jsonPointerExpr: Option[String],
    jsonArrayPointers: Map[String, Map[String, String]]
) extends BlobListingStreamingSource[PathType](
      sourcePath,
      shardStoragePath,
      storageClient,
      nameGenerator,
      primaryKeys,
      tempStoragePath
    ):

  private def sourceSchema: Task[AvroSchema] = for
    parser <- ZIO.succeed(org.apache.avro.Schema.Parser())
    schema <- ZIO
      .attempt(parser.parse(avroSchemaString))
      .orDieWith(e => Throwable("Invalid Avro schema provided for source", e))
  yield schema

  override def getSchema: Task[SchemaType] = for arcaneSchema <- sourceSchema.map(implicitly)
  yield arcaneSchema ++ Seq(BlobBatchCommons.versionField)

  /** Gets an empty schema.
    *
    * @return
    *   An empty schema.
    */
  override def empty: SchemaType = ArcaneSchema.empty()

  def fileToStream(sourceFile: StoredBlob, schema: ArcaneSchema): Task[StructuredZStream] = for
    downloadedFilePath <- downloadSourceFile(sourceFile)
    avroSchema         <- sourceSchema
    scanner            <- ZIO.attempt(JsonScanner(downloadedFilePath, avroSchema, jsonPointerExpr, jsonArrayPointers))
  yield (
    scanner.getRows.map(
      BlobBatchCommons.enrichBatchRow(_, sourceFile.createdOn.getOrElse(0), primaryKeys, mergeKeyHasher())
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
                filePath   <- downloadSourceFile(sourceFile)
                avroSchema <- sourceSchema
                scanner    <- ZIO.attempt(JsonScanner(filePath, avroSchema, jsonPointerExpr, jsonArrayPointers))
              yield scanner
            }
            .flatMap(
              _.getRows.map(
                BlobBatchCommons.enrichBatchRow(_, sourceFile.createdOn.getOrElse(0), primaryKeys, mergeKeyHasher())
              )
            )
        },
      schema
    )
object BlobListingJsonStreamingSource:
  private type SettingsExtractor = PluginStreamContext => JsonBlobSourceSettings

  /** Default layer is S3. Provide your own layer (Azure etc.) through plugin override if needed
    */
  def getS3Layer(
      extractor: SettingsExtractor
  ): ZLayer[S3BlobStorageService & NameGenerator & PluginStreamContext, Throwable, BlobListingJsonStreamingSource[
    S3StoragePath
  ]] =
    ZLayer {
      for
        context        <- ZIO.service[PluginStreamContext]
        storageService <- ZIO.service[S3BlobStorageService]
        sourceSettings <- ZIO.attempt(extractor(context))
        nameGenerator  <- ZIO.service[NameGenerator]
        sourcePath <- ZIO.getOrFailWith(new IllegalArgumentException("Invalid S3 source path provided"))(
          S3StoragePath(sourceSettings.sourcePath).toOption
        )
        shardStoragePath <- ZIO.getOrFailWith(new IllegalArgumentException("Invalid S3 shard storage path provided"))(
          S3StoragePath(sourceSettings.shardStoragePath).toOption
        )
      yield new BlobListingJsonStreamingSource(
        sourcePath = sourcePath,
        shardStoragePath = shardStoragePath,
        storageClient = storageService,
        tempStoragePath = sourceSettings.tempStoragePath,
        primaryKeys = sourceSettings.primaryKeys,
        nameGenerator = nameGenerator,
        avroSchemaString = sourceSettings.avroSchemaString,
        jsonPointerExpr = sourceSettings.jsonPointerExpression,
        jsonArrayPointers = sourceSettings.jsonArrayPointers
      )
    }
