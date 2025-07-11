package com.sneaksanddata.arcane.framework
package services.blobsource.readers.listing

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

import org.apache.iceberg.data.GenericRecord
import zio.stream.{ZSink, ZStream}
import zio.{Task, ZIO, ZLayer}

import java.security.MessageDigest
import java.time.{Duration, OffsetDateTime}
import java.util.Base64

class BlobListingParquetSource[PathType <: BlobPath](
    sourcePath: PathType,
    reader: BlobStorageReader[PathType],
    tempStoragePath: String,
    primaryKeys: Seq[String]
) extends BlobSourceReader
    with SchemaProvider[ArcaneSchema]:

  override type OutputRow = DataRow

  private val mergeKeyHasher                        = MessageDigest.getInstance("SHA-256")
  private def encodeHash(hash: Array[Byte]): String = Base64.getEncoder.encodeToString(hash)

  private def getMergeKeyValue(row: DataRow, keys: Seq[String]): String = encodeHash(
    mergeKeyHasher.digest(
      keys
        .map { key =>
          row.find(cell => cell.name == key) match
            case Some(pkCell) => pkCell.value.toString
            case None =>
              throw new RuntimeException(s"Primary key $key does not exist in the rows emitted by this source")
        }
        .mkString
        .toLowerCase
        .getBytes("UTF-8")
    )
  )

  private def enrichedWithMergeKey(row: DataRow): DataRow = row ++ Seq(
    DataCell(
      name = MergeKeyField.name,
      Type = MergeKeyField.fieldType,
      value = getMergeKeyValue(row, primaryKeys)
    )
  )

  override def getSchema: Task[SchemaType] = for
    filePath <- reader.downloadRandomBlob(sourcePath, tempStoragePath)
    scanner  <- ZIO.attempt(ParquetScanner(filePath))
    schema   <- scanner.getIcebergSchema.map(implicitly)
  yield schema

  /** Gets an empty schema.
    *
    * @return
    *   An empty schema.
    */
  override def empty: SchemaType = ArcaneSchema.empty()

  override def getChanges(startFrom: Long): ZStream[Any, Throwable, (OutputRow, Long)] = for
    sourceFile <- reader.streamPrefixes(sourcePath)
    downloadedFile <- ZStream.fromZIO(
      reader.downloadBlob(s"${sourcePath.protocol}://${sourceFile.name}", tempStoragePath)
    )
    scanner <- ZStream.fromZIO(ZIO.attempt(ParquetScanner(downloadedFile)))
    row     <- scanner.getRows.map(enrichedWithMergeKey)
  yield (row, sourceFile.createdOn.getOrElse(0))

  // Listing readers do not support versioned streams, since they do not keep track of which file has been or not been processed
  // thus they always act like they lookback until beginning of time
  override def getStartFrom(lookBackInterval: Duration): Task[Long] = ZIO.succeed(
    OffsetDateTime
      .now()
      .minus(lookBackInterval)
      .toInstant
      .toEpochMilli
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
