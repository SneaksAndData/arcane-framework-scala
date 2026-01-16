package com.sneaksanddata.arcane.framework
package services.synapse

import logging.ZIOLogAnnotations.zlogStream
import services.storage.base.BlobStorageReader
import services.storage.models.azure.AdlsStoragePath
import services.storage.models.base.StoredBlob

import zio.stream.ZStream

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import scala.util.Try

object SynapseAzureBlobReaderExtensions:

  /** Converts a blob name to a valid offset datetime
    * @param blob
    *   A Synapse Link blob prefix (date folder)
    * @return
    */
  extension (blob: StoredBlob)
    private def interpretAsDate: Option[OffsetDateTime] =
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH.mm.ssX")
      Try(OffsetDateTime.parse(blob.asFolderName, formatter)).toOption

    def asFolderName: String = blob.name.replaceAll("/$", "")
    
    def asDate: OffsetDateTime = interpretAsDate.get

  /** Read a list of the prefixes, taking optional start time. Lowest precision available is 1 hour
    * @return
    *   A stream of root prefixes and the latest change date associated with them
    */
  extension (reader: BlobStorageReader[AdlsStoragePath])
    def getEligibleDates(
        storagePath: AdlsStoragePath,
        startFrom: OffsetDateTime
    ): ZStream[Any, Throwable, StoredBlob] = for
      _ <- zlogStream("Getting root prefixes starting from %s", startFrom.toString)
      // changelog.info indicates which batch is in progress right now - thus we remove it from eligible prefixes to avoid reading incomplete data
      inProgressDate <- ZStream.fromZIO(reader.readBlobContent(storagePath + "Changelog/changelog.info"))
      inProgressDateParsed <- ZStream.succeed(
        OffsetDateTime.parse(inProgressDate, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH.mm.ssX"))
      )
      prefix <- ZStream.fromIterable(getPrefixesList(startFrom, inProgressDateParsed))
      eligibleBlob <- reader
        .streamPrefixes(storagePath + prefix)
        .map(blob => (blob.interpretAsDate, blob))
        .collect {
          case (Some(date), blob)
              if (date.isAfter(startFrom) || date.isEqual(startFrom)) && (!date.isEqual(inProgressDateParsed)) => blob
        }
    yield eligibleBlob

private def getPrefixesList(startDate: OffsetDateTime, endDate: OffsetDateTime): Seq[String] =
  val currentMoment = endDate.plusHours(1)
  val startMoment   = startDate
  Iterator
    .iterate(startMoment)(_.plusHours(1))
    .takeWhile(_.toEpochSecond < currentMoment.toEpochSecond)
    .map { moment =>
      val monthString = s"00${moment.getMonth.getValue}".takeRight(2)
      val dayString   = s"00${moment.getDayOfMonth}".takeRight(2)
      val hourString  = s"00${moment.getHour}".takeRight(2)
      s"${moment.getYear}-$monthString-${dayString}T$hourString"
    }
    .to(LazyList)
