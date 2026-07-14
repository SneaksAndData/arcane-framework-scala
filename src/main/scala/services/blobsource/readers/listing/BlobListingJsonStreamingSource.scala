package com.sneaksanddata.arcane.framework
package services.blobsource.readers.listing

import models.app.PluginStreamContext
import models.batches.BlobBatchCommons
import models.schemas.{ArcaneSchema, DataRow}
import models.settings.sources.blob.JsonBlobSourceSettings
import services.blobsource.versioning.BlobSourceWatermark
import services.iceberg.given_Conversion_AvroSchema_ArcaneSchema
import services.iceberg.interop.JsonScanner
import services.storage.base.BlobStorageReader
import services.storage.models.base.{BlobPath, StoredBlob}
import services.storage.models.s3.S3StoragePath
import services.storage.services.s3.S3BlobStorageService
import services.streaming.base.StructuredZStream

import org.apache.avro.Schema as AvroSchema
import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

class BlobListingJsonStreamingSource[PathType <: BlobPath](
    sourcePath: PathType,
    reader: BlobStorageReader[PathType],
    tempStoragePath: String,
    primaryKeys: Seq[String],
    avroSchemaString: String,
    jsonPointerExpr: Option[String],
    jsonArrayPointers: Map[String, Map[String, String]]
) extends BlobListingStreamingSource[PathType](sourcePath, reader, primaryKeys):

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

  override def getChanges(startFrom: BlobSourceWatermark): ZStream[Any, Throwable, StructuredZStream] = reader
    .streamPrefixes(sourcePath)
    .filter(_.createdOn.map(BlobSourceWatermark.fromEpochSecond).getOrElse(BlobSourceWatermark.epoch) >= startFrom)
    .mapZIO(fileToStream)

  def fileToStream(sourceFile: StoredBlob): Task[StructuredZStream] =
    reader
      .downloadBlob(s"${sourcePath.protocol}://${sourceFile.name}", tempStoragePath)
      .flatMap(v => sourceSchema.map(schema => (schema, v)))
      .flatMap { case (avroSchema, filePath) =>
        getSchema.map { schema =>
          (
            JsonScanner(filePath, avroSchema, jsonPointerExpr, jsonArrayPointers).getRows.map(
              BlobBatchCommons.enrichBatchRow(_, sourceFile.createdOn.getOrElse(0), primaryKeys, mergeKeyHasher())
            ),
            schema
          )
        }
      }

  override def filesToStream(sourceFiles: Seq[StoredBlob]): Task[(ZStream[Any, Throwable, DataRow], ArcaneSchema)] =
    getSchema.map { schema =>
      val stream = ZStream
        .fromIterable(sourceFiles)
        .flatMap { sourceFile =>
          ZStream
            .fromZIO {
              for
                filePath   <- reader.downloadBlob(s"${sourcePath.protocol}://${sourceFile.name}", tempStoragePath)
                avroSchema <- sourceSchema
                scanner    <- ZIO.attempt(JsonScanner(filePath, avroSchema, jsonPointerExpr, jsonArrayPointers))
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

object BlobListingJsonStreamingSource:
  def apply(
             sourcePath: S3StoragePath,
             s3Reader: S3BlobStorageService,
             tempPath: String,
             primaryKeys: Seq[String],
             avroSchemaString: String,
             jsonPointerExpr: Option[String],
             jsonArrayPointers: Map[String, Map[String, String]]
  ): BlobListingJsonStreamingSource[S3StoragePath] =
    new BlobListingJsonStreamingSource[S3StoragePath](
      sourcePath,
      s3Reader,
      tempPath,
      primaryKeys,
      avroSchemaString,
      jsonPointerExpr,
      jsonArrayPointers
    )

  private type SettingsExtractor = PluginStreamContext => JsonBlobSourceSettings

  /** Default layer is S3. Provide your own layer (Azure etc.) through plugin override if needed
    */
  def getLayer(
      extractor: SettingsExtractor
  ): ZLayer[S3BlobStorageService & PluginStreamContext, Throwable, BlobListingJsonStreamingSource[S3StoragePath]] =
    ZLayer {
      for
        context        <- ZIO.service[PluginStreamContext]
        blobReader     <- ZIO.service[S3BlobStorageService]
        sourceSettings <- ZIO.attempt(extractor(context))
        sourcePath <- ZIO.getOrFailWith(new IllegalArgumentException("Invalid S3 path provided"))(
          S3StoragePath(sourceSettings.sourcePath).toOption
        )
      yield BlobListingJsonStreamingSource(
        sourcePath,
        blobReader,
        sourceSettings.tempStoragePath,
        sourceSettings.primaryKeys,
        sourceSettings.avroSchemaString,
        sourceSettings.jsonPointerExpression,
        sourceSettings.jsonArrayPointers
      )
    }
