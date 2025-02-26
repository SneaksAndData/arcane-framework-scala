package com.sneaksanddata.arcane.framework
package services.cdm

import logging.ZIOLogAnnotations.zlogStream
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

  private type BlobStream = ZStream[Any, Throwable, StoredBlob]

  private type SchemaEnrichedBlobStream = ZStream[Any, Throwable, SchemaEnrichedBlob]

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
  extension (stream: BlobStream) def enrichWithSchema(azureBlobStorageReader: AzureBlobStorageReader, storagePath: AdlsStoragePath, name: String): SchemaEnrichedBlobStream =
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
  extension (reader: BlobStorageReader[AdlsStoragePath]) def getRootPrefixes(storagePath: AdlsStoragePath, startFrom: OffsetDateTime): BlobStream =
    for _ <- zlogStream("Getting root prefixes stating from " + startFrom)
        list <- ZStream.succeed(getListPrefixes(Some(startFrom)))
        listZIO = ZIO.foreach(list)(prefix => ZIO.attemptBlocking { reader.streamPrefixes(storagePath + prefix) })
        prefixes <- ZStream.fromIterableZIO(listZIO)
        zippedWithDate <- prefixes.map(blob => (interpretAsDate(blob), blob))
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

  private def getListPrefixes(startDate: Option[OffsetDateTime]): Seq[String] =
    val defaultFromYears: Int = 1
    val currentMoment = OffsetDateTime.now(ZoneOffset.UTC).plusHours(1)
    val startMoment = startDate.getOrElse(currentMoment.minusYears(defaultFromYears)).minus(Duration.ofHours(1))
    Iterator.iterate(startMoment)(_.plusHours(1))
      .takeWhile(_.toEpochSecond < currentMoment.toEpochSecond)
      .map { moment =>
        val monthString = s"00${moment.getMonth.getValue}".takeRight(2)
        val dayString = s"00${moment.getDayOfMonth}".takeRight(2)
        val hourString = s"00${moment.getHour}".takeRight(2)
        s"${moment.getYear}-$monthString-${dayString}T$hourString"
      }.to(LazyList)
