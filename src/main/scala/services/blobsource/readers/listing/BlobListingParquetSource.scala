package com.sneaksanddata.arcane.framework
package services.blobsource.readers.listing

import models.batches.BlobBatchCommons
import models.schemas.{*, given}
import models.settings.BlobSourceSettings
import services.base.SchemaProvider
import services.blobsource.readers.BlobSourceReader
import services.iceberg.given_Conversion_Schema_ArcaneSchema
import services.iceberg.interop.ParquetScanner
import services.storage.base.BlobStorageReader
import services.storage.models.base.BlobPath
import services.storage.models.s3.S3StoragePath
import services.storage.services.s3.S3BlobStorageReader

import zio.stream.{ZSink, ZStream}
import zio.{Task, ZIO, ZLayer}

import java.time.{Duration, OffsetDateTime}

class BlobListingParquetSource[PathType <: BlobPath](
    sourcePath: PathType,
    reader: BlobStorageReader[PathType],
    tempStoragePath: String,
    primaryKeys: Seq[String]
) extends BlobSourceReader
    with SchemaProvider[ArcaneSchema]:

  override type OutputRow = DataRow

  override def getSchema: Task[SchemaType] = for
    filePath <- reader.downloadRandomBlob(sourcePath, tempStoragePath)
    scanner  <- ZIO.attempt(ParquetScanner(filePath))
    schema   <- scanner.getIcebergSchema.map(implicitly)
  yield schema ++ Seq(BlobBatchCommons.versionField)

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
    scanner <- ZStream.fromZIO(ZIO.attempt(ParquetScanner(downloadedFile)))
    row     <- scanner.getRows.map(BlobBatchCommons.enrichBatchRow(_, sourceFile.createdOn.getOrElse(0), primaryKeys))
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

object BlobListingParquetSource:
  def apply(
      sourcePath: S3StoragePath,
      s3Reader: S3BlobStorageReader,
      tempPath: String,
      primaryKeys: Seq[String]
  ): BlobListingParquetSource[S3StoragePath] =
    new BlobListingParquetSource[S3StoragePath](sourcePath, s3Reader, tempPath, primaryKeys)

  /** Default layer is S3. Provide your own layer (Azure etc.) through plugin override if needed
    */
  val layer: ZLayer[BlobSourceSettings & S3BlobStorageReader, IllegalArgumentException, BlobListingParquetSource[
    S3StoragePath
  ]] = ZLayer {
    for
      blobReader     <- ZIO.service[S3BlobStorageReader]
      sourceSettings <- ZIO.service[BlobSourceSettings]
      sourcePath <- ZIO.getOrFailWith(new IllegalArgumentException("Invalid S3 path provided"))(
        S3StoragePath(sourceSettings.sourcePath).toOption
      )
    yield BlobListingParquetSource(sourcePath, blobReader, sourceSettings.tempStoragePath, sourceSettings.primaryKeys)
  }
