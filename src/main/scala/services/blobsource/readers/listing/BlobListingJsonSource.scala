package com.sneaksanddata.arcane.framework
package services.blobsource.readers.listing

import models.batches.BlobBatchCommons
import models.schemas.{ArcaneSchema, DataRow, given_CanAdd_ArcaneSchema}
import models.settings.blob.JsonBlobSourceSettings
import services.base.SchemaProvider
import services.iceberg.given_Conversion_AvroSchema_ArcaneSchema
import services.iceberg.interop.JsonScanner
import services.storage.base.BlobStorageReader
import services.storage.models.base.BlobPath
import services.storage.models.s3.S3StoragePath
import services.storage.services.s3.S3BlobStorageReader

import org.apache.avro.Schema as AvroSchema
import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

import java.security.MessageDigest

class BlobListingJsonSource[PathType <: BlobPath](
    sourcePath: PathType,
    reader: BlobStorageReader[PathType],
    tempStoragePath: String,
    primaryKeys: Seq[String],
    avroSchemaString: String
) extends BlobListingSource[PathType](sourcePath, reader, primaryKeys)
    with SchemaProvider[ArcaneSchema]:

  private val mergeKeyHasher = MessageDigest.getInstance("SHA-256")

  override type OutputRow = DataRow

  private def sourceSchema: Task[AvroSchema] = for parser <- ZIO.succeed(org.apache.avro.Schema.Parser())
  yield parser.parse(avroSchemaString)

  override def getSchema: Task[SchemaType] = for arcaneSchema <- sourceSchema.map(implicitly)
  yield arcaneSchema ++ Seq(BlobBatchCommons.versionField)

  /** Gets an empty schema.
    *
    * @return
    *   An empty schema.
    */
  override def empty: SchemaType = ArcaneSchema.empty()

  override def getChanges(startFrom: Long): ZStream[Any, Throwable, (DataRow, Long)] = for
    sourceFile <- reader.streamPrefixes(sourcePath).filter(_.createdOn.getOrElse(0L) >= startFrom)
    downloadedFile <- ZStream.fromZIO(
      reader.downloadBlob(s"${sourcePath.protocol}://${sourceFile.name}", tempStoragePath)
    )
    schema  <- ZStream.fromZIO(sourceSchema)
    scanner <- ZStream.succeed(JsonScanner(downloadedFile, schema))
    row <- scanner.getRows.map(
      BlobBatchCommons.enrichBatchRow(_, sourceFile.createdOn.getOrElse(0), primaryKeys, mergeKeyHasher)
    )
  yield (row, sourceFile.createdOn.getOrElse(0L))

object BlobListingJsonSource:
  def apply(
      sourcePath: S3StoragePath,
      s3Reader: S3BlobStorageReader,
      tempPath: String,
      primaryKeys: Seq[String],
      avroSchemaString: String
  ): BlobListingJsonSource[S3StoragePath] =
    new BlobListingJsonSource[S3StoragePath](sourcePath, s3Reader, tempPath, primaryKeys, avroSchemaString)

  /** Default layer is S3. Provide your own layer (Azure etc.) through plugin override if needed
    */
  val layer: ZLayer[JsonBlobSourceSettings & S3BlobStorageReader, IllegalArgumentException, BlobListingJsonSource[
    S3StoragePath
  ]] = ZLayer {
    for
      blobReader     <- ZIO.service[S3BlobStorageReader]
      sourceSettings <- ZIO.service[JsonBlobSourceSettings]
      sourcePath <- ZIO.getOrFailWith(new IllegalArgumentException("Invalid S3 path provided"))(
        S3StoragePath(sourceSettings.sourcePath).toOption
      )
    yield BlobListingJsonSource(
      sourcePath,
      blobReader,
      sourceSettings.tempStoragePath,
      sourceSettings.primaryKeys,
      sourceSettings.avroSchemaString
    )
  }
