package com.sneaksanddata.arcane.framework
package services.cdm

import extensions.ZStreamExtensions.dropLast
import logging.ZIOLogAnnotations.{zlog, zlogStream}
import models.ArcaneSchema
import services.base.SchemaProvider
import services.storage.base.BlobStorageReader
import services.storage.models.azure.AdlsStoragePath
import services.storage.models.base.StoredBlob
import services.storage.services.AzureBlobStorageReader

import zio.stream.ZStream
import zio.{Task, ZIO}

import java.time.format.DateTimeFormatter
import java.time.{Duration, OffsetDateTime, ZoneOffset}
import scala.util.Try

case class SchemaEnrichedBlob(blob: StoredBlob, schemaProvider: SchemaProvider[ArcaneSchema])

/**
 * Extension methods for AzureBlobStorageReader specific to Synapse Link append-only tables export format
 */
object AzureBlobStorageReaderExtensions:

  type SchemaEnrichedBlobStream = ZStream[Any, Throwable, SchemaEnrichedBlob]

  private type BlobStream = ZStream[Any, Throwable, StoredBlob]

  extension (reader: BlobStorageReader[AdlsStoragePath]) def getLastCommitDate(storageRoot: AdlsStoragePath): Task[OffsetDateTime] =
    ZIO.scoped {
      for
        reader <- ZIO.fromAutoCloseable(reader.streamBlobContent(storageRoot + "Changelog/changelog.info"))
        text <- ZIO.attemptBlocking(reader.readLine())
        _ <- zlog(s"Read latest prefix from changelog.info: $text")
        latestPrefix = OffsetDateTime.parse(text, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH.mm.ssX"))
      yield latestPrefix
    }


  /**
   * Stream the content of the table from the Azure Blob Storage.
   * Also, this method is used in the backfill process.
   *
   * @param reader The reader for the Azure Blob Storage.
   * @param storagePath The path to the storage account
   * @param startDate Folders from Synapse export to include in the snapshot, based on the start date provided.
   * @param tableName The name of the table.
   * @return A stream of file paths contains list of CSV files belonging to a specific table starting from date in the storage path.
   */
  extension (reader: BlobStorageReader[AdlsStoragePath]) def streamTableContent(storagePath: AdlsStoragePath, startDate: OffsetDateTime, endDate: OffsetDateTime, tableName: String): SchemaEnrichedBlobStream =
    reader.getRootPrefixes(storagePath, startDate, endDate).dropLast
      .enrichWithSchema(reader, storagePath, tableName)
      .flatMap(seb => reader.streamPrefixes(storagePath + seb.blob.name).addSchema(seb.schemaProvider))
      .filterByTableName(tableName)
      .flatMap(seb => reader.streamPrefixes(storagePath + seb.blob.name).addSchema(seb.schemaProvider))
      .onlyCSVs

  /**
   * The shorthand method for filtering out the CSV files from the stream
   * @return The stream that contains only exact matches for the table name.
   */
  extension (stream: SchemaEnrichedBlobStream) def onlyCSVs: SchemaEnrichedBlobStream =
    stream.filter(schemaEnrichedBlob => schemaEnrichedBlob.blob.name.endsWith(".csv"))

  /**
   * The method we use to filter blob streams can lead to a case when two different tables are stored with the same
   * prefix, e.g. `table1` and `table11`. This method filters out the prefixes that are not the exact match for
   * the table name.
   * @param tableName The name of the table
   * @return The stream that contains only exact matches for the table name.
   */
  extension (stream: SchemaEnrichedBlobStream) def filterByTableName(tableName: String): SchemaEnrichedBlobStream =
    stream.filter(schemaEnrichedBlob => schemaEnrichedBlob.blob.name.endsWith(s"/$tableName/"))
    
  /**
   * Add schema information to the stream
   * This is shorthand method for adding schema information to the stream
   * @param schemaProvider The schema provider
   * @return A stream of blobs enriched with schema information
   */
  extension (stream: BlobStream) def addSchema(schemaProvider: SchemaProvider[ArcaneSchema]): SchemaEnrichedBlobStream =
    stream.map(blob => SchemaEnrichedBlob(blob, schemaProvider))

  /**
   * Enrich the stream with schema information
   * This methods reads the schema from the model.json file in the same folder as the blob
   * @param azureBlobStorageReader The reader for the Azure Blob Storage
   * @param storagePath The location of the table
   * @param name The name of the table
   * @return A stream of blobs enriched with schema information
   */
  extension (stream: BlobStream) def enrichWithSchema(azureBlobStorageReader: BlobStorageReader[AdlsStoragePath], storagePath: AdlsStoragePath, name: String): SchemaEnrichedBlobStream =
    stream.filterZIO(prefix => azureBlobStorageReader.blobExists(storagePath + prefix.name + "model.json")).map(prefix => {
      val schemaProvider = CdmSchemaProvider(azureBlobStorageReader, (storagePath + prefix.name).toHdfsPath, name)
      SchemaEnrichedBlob(prefix, schemaProvider)
    })


  /**
   * Read a list of the prefixes, taking optional start time. Lowest precision available is 1 hour
   * @param storagePath The path to the storage account
   * @param startFrom Folders from Synapse export to include in the snapshot, based on the start date provided.
   * @return A stream of root prefixes
   */
  extension (reader: BlobStorageReader[AdlsStoragePath]) def getRootPrefixes(storagePath: AdlsStoragePath, startFrom: OffsetDateTime, endDate: OffsetDateTime): BlobStream =
    for _ <- zlogStream("Getting root prefixes stating from " + startFrom)
        prefix <- ZStream.fromIterable(getListPrefixes(startFrom, endDate))
        blob <- reader.streamPrefixes(storagePath + prefix)
        zippedWithDate = (interpretAsDate(blob), blob)
        eligibleToProcess <- zippedWithDate match
          case (Some(date), blob) if date.isAfter(startFrom) || date.isEqual(startFrom) => ZStream.succeed(blob)
          case _ => ZStream.empty
    yield eligibleToProcess

  extension (reader: AzureBlobStorageReader) def getFirstDropDate(storagePath: AdlsStoragePath): Task[OffsetDateTime] =
    reader.streamPrefixes(storagePath).runFold(OffsetDateTime.now(ZoneOffset.UTC)) { (date, blob) =>
      val current = interpretAsDate(blob).getOrElse(date)
      if current.isBefore(date) then current else date
    }

  private def interpretAsDate(blob: StoredBlob): Option[OffsetDateTime] =
    val name = blob.name.replaceAll("/$", "")
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH.mm.ssX")
    Try(OffsetDateTime.parse(name, formatter)).toOption

  private def getListPrefixes(startDate: OffsetDateTime, endDate: OffsetDateTime): Seq[String] =
    val defaultFromYears: Int = 1
    val currentMoment = endDate
    val startMoment = startDate
    Iterator.iterate(startMoment)(_.plusHours(1))
      .takeWhile(_.toEpochSecond < currentMoment.toEpochSecond)
      .map { moment =>
        val monthString = s"00${moment.getMonth.getValue}".takeRight(2)
        val dayString = s"00${moment.getDayOfMonth}".takeRight(2)
        val hourString = s"00${moment.getHour}".takeRight(2)
        s"${moment.getYear}-$monthString-${dayString}T$hourString"
      }.to(LazyList)
