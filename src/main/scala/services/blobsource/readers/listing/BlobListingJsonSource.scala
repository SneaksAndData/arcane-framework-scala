package com.sneaksanddata.arcane.framework
package services.blobsource.readers.listing

import models.batches.BlobBatchCommons
import models.schemas.{ArcaneSchema, DataRow, given_CanAdd_ArcaneSchema}
import services.base.SchemaProvider
import services.blobsource.readers.BlobSourceReader
import services.iceberg.given_Conversion_AvroSchema_ArcaneSchema
import services.iceberg.interop.JsonScanner
import services.storage.base.BlobStorageReader
import services.storage.models.base.BlobPath

import org.apache.avro.Schema as AvroSchema
import zio.stream.{ZSink, ZStream}
import zio.{Task, ZIO}

import java.time.{Duration, OffsetDateTime}

class BlobListingJsonSource[PathType <: BlobPath](
                                                   sourcePath: PathType,
                                                   reader: BlobStorageReader[PathType],
                                                   tempStoragePath: String,
                                                   primaryKeys: Seq[String],
                                                   avroSchemaString: String
) extends BlobSourceReader
    with SchemaProvider[ArcaneSchema]:

  override type OutputRow = DataRow
  
  private def sourceSchema: Task[AvroSchema] = for
     parser <- ZIO.succeed(org.apache.avro.Schema.Parser())
  yield parser.parse(avroSchemaString)

  override def getSchema: Task[SchemaType] = for 
      arcaneSchema <- sourceSchema.map(implicitly)
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
    schema <- ZStream.fromZIO(sourceSchema)
    scanner <- ZStream.succeed(JsonScanner(downloadedFile, schema))
    row <- scanner.getRows.map(BlobBatchCommons.enrichBatchRow(_, sourceFile.createdOn.getOrElse(0), primaryKeys))
  yield (row, sourceFile.createdOn.getOrElse(0L))

  // Listing readers do not support versioned streams, since they do not keep track of which file has been or not been processed
  // thus they always act like they lookback until beginning of time
  override def getStartFrom(lookBackInterval: Duration): Task[Long] = ZIO.succeed(
    OffsetDateTime
      .now()
      .minus(lookBackInterval)
      .toInstant
      .toEpochMilli / 1000
  )

  override def getLatestVersion: Task[Long] = reader
    .streamPrefixes(sourcePath)
    .map(_.createdOn.getOrElse(0L))
    .run(ZSink.foldLeft(0L)((e, agg) => if (e > agg) e else agg))
