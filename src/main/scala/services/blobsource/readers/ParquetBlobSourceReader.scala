package com.sneaksanddata.arcane.framework
package services.blobsource.readers

import models.schemas.*
import models.schemas.given
import services.base.SchemaProvider
import services.iceberg.given_Conversion_Schema_ArcaneSchema
import services.iceberg.interop.ParquetScanner
import services.storage.base.BlobStorageReader
import services.storage.models.base.BlobPath

import org.apache.iceberg.data.GenericRecord
import zio.stream.ZStream
import zio.{Task, ZIO}

import java.security.MessageDigest
import java.util.Base64

class ParquetBlobSourceReader[PathType <: BlobPath](
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
            case Some(pkCell) => pkCell.value
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

  override def getChanges(startFrom: Long): ZStream[Any, Throwable, OutputRow] = for
    sourceFile <- reader.streamPrefixes(sourcePath)
    downloadedFile <- ZStream.fromZIO(
      reader.downloadBlob(s"${sourcePath.protocol}://${sourceFile.name}", tempStoragePath)
    )
    scanner <- ZStream.fromZIO(ZIO.attempt(ParquetScanner(downloadedFile)))
    row     <- scanner.getRows.map(enrichedWithMergeKey)
  yield row
