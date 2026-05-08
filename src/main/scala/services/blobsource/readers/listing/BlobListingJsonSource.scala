package com.sneaksanddata.arcane.framework
package services.blobsource.readers.listing

import models.app.PluginStreamContext
import models.batches.BlobBatchCommons
import models.schemas.{ArcaneSchema, DataRow, given_CanAdd_ArcaneSchema}
import models.settings.sources.blob.JsonBlobSourceSettings
import services.base.SchemaProvider
import services.blobsource.versioning.BlobSourceWatermark
import services.iceberg.given_Conversion_AvroSchema_ArcaneSchema
import services.iceberg.interop.JsonScanner
import services.storage.base.BlobStorageReader
import services.storage.models.base.BlobPath
import services.storage.models.s3.S3StoragePath
import services.storage.services.s3.S3BlobStorageReader

import com.sneaksanddata.arcane.framework.services.streaming.base.StructuredZStream
import org.apache.avro.Schema as AvroSchema
import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

class BlobListingJsonSource[PathType <: BlobPath](
    sourcePath: PathType,
    reader: BlobStorageReader[PathType],
    tempStoragePath: String,
    primaryKeys: Seq[String],
    avroSchemaString: String,
    jsonPointerExpr: Option[String],
    jsonArrayPointers: Map[String, Map[String, String]]
) extends BlobListingSource[PathType](sourcePath, reader, primaryKeys)
    with SchemaProvider[ArcaneSchema]:

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
    .mapZIO { sourceFile =>
      reader.downloadBlob(s"${sourcePath.protocol}://${sourceFile.name}", tempStoragePath)
        .flatMap(v => sourceSchema.map(schema => (schema, v)))
        .map { case (schema, filePath) => (JsonScanner(filePath, schema, jsonPointerExpr, jsonArrayPointers), sourceFile) }
    }
    .mapZIO { case (scanner, sourceFile) =>
      getSchema.map {schema =>
        (scanner.getRows.map(
          BlobBatchCommons.enrichBatchRow(_, sourceFile.createdOn.getOrElse(0), primaryKeys, mergeKeyHasher)
        ), schema) 
      }
    }

object BlobListingJsonSource:
  def apply(
      sourcePath: S3StoragePath,
      s3Reader: S3BlobStorageReader,
      tempPath: String,
      primaryKeys: Seq[String],
      avroSchemaString: String,
      jsonPointerExpr: Option[String],
      jsonArrayPointers: Map[String, Map[String, String]]
  ): BlobListingJsonSource[S3StoragePath] =
    new BlobListingJsonSource[S3StoragePath](
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
  ): ZLayer[S3BlobStorageReader & PluginStreamContext, Throwable, BlobListingJsonSource[S3StoragePath]] = ZLayer {
    for
      context        <- ZIO.service[PluginStreamContext]
      blobReader     <- ZIO.service[S3BlobStorageReader]
      sourceSettings <- ZIO.attempt(extractor(context))
      sourcePath <- ZIO.getOrFailWith(new IllegalArgumentException("Invalid S3 path provided"))(
        S3StoragePath(sourceSettings.sourcePath).toOption
      )
    yield BlobListingJsonSource(
      sourcePath,
      blobReader,
      sourceSettings.tempStoragePath,
      sourceSettings.primaryKeys,
      sourceSettings.avroSchemaString,
      sourceSettings.jsonPointerExpression,
      sourceSettings.jsonArrayPointers
    )
  }
