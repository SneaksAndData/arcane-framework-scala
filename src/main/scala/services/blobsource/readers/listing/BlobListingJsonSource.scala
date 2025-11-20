package com.sneaksanddata.arcane.framework
package services.blobsource.readers.listing

import models.batches.BlobBatchCommons
import models.schemas.*
import services.base.SchemaProvider
import services.blobsource.readers.BlobSourceReader
import services.iceberg.interop.JsonScanner
import services.iceberg.{
  given_Conversion_AvroGenericRecord_DataRow,
  given_Conversion_AvroSchema_ArcaneSchema,
  given_Conversion_AvroType_ArcaneType
}
import services.storage.base.BlobStorageReader
import services.storage.models.base.BlobPath
import services.storage.models.s3.S3StoragePath

import org.apache.avro.Schema as AvroSchema
import org.apache.iceberg.avro.AvroSchemaUtil
import zio.stream.ZStream
import zio.{Task, ZIO}

import java.security.MessageDigest
import java.time.Duration
import java.util.Base64

class BlobListingJsonSource[PathType <: BlobPath](
                                                   sourcePath: PathType,
                                                   reader: BlobStorageReader[PathType],
                                                   tempStoragePath: String,
                                                   primaryKeys: Seq[String],
                                                   avroSchemaString: String
) extends BlobSourceReader
    with SchemaProvider[ArcaneSchema]:

  override type OutputRow = DataRow
  
  private def getAvroSchema: Task[AvroSchema] = for
     parser <- ZIO.succeed(org.apache.avro.Schema.Parser())
  yield parser.parse(avroSchemaString)

  override def getSchema: Task[SchemaType] = for 
      arcaneSchema <- getAvroSchema.map(implicitly)
  yield arcaneSchema ++ Seq(BlobBatchCommons.versionField)

  /** Gets an empty schema.
    *
    * @return
    *   An empty schema.
    */
  override def empty: SchemaType = ArcaneSchema.empty()

  override def getChanges(startFrom: Long): ZStream[Any, Throwable, (OutputRow, Long)] = for
    sourceFile <- reader.streamPrefixes(sourcePath).filter(_.createdOn.getOrElse(0L) >= startFrom)
    downloadedFile <- ZStream.fromZIO(
      reader.downloadBlob(s"${sourcePath.protocol}://${sourceFile.name}", tempStoragePath)
    )
    scanner <- ZStream.fromZIO(ZIO.attempt(JsonScanner(downloadedFile)))
    row     <- scanner.getRows.map(BlobBatchCommons.enrichBatchRow(_, sourceFile.createdOn.getOrElse(0), primaryKeys))
  yield (row, sourceFile.createdOn.getOrElse(0L))

  override def getStartFrom(lookBackInterval: Duration): Task[Long] = ZIO.succeed(0)

  override def getLatestVersion: Task[Long] = ZIO.succeed(0)
