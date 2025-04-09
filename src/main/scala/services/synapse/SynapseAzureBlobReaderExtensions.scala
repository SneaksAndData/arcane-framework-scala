package com.sneaksanddata.arcane.framework
package services.synapse

import services.storage.base.BlobStorageReader
import services.storage.models.azure.AdlsStoragePath

import com.sneaksanddata.arcane.framework.logging.ZIOLogAnnotations.zlogStream
import com.sneaksanddata.arcane.framework.services.storage.models.base.StoredBlob
import zio.{Task, ZIO}
import zio.stream.ZStream

import java.time.{Duration, OffsetDateTime}
import java.time.format.DateTimeFormatter
import scala.util.Try

object SynapseAzureBlobReaderExtensions:

  /**
   * Converts a blob name to a valid offset datetime
   * @param blob A Synapse Link blob prefix (date folder)
   * @return
   */
  private def interpretAsDate(blob: StoredBlob): Option[OffsetDateTime] =
    val name = blob.name.replaceAll("/$", "")
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH.mm.ssX")
    Try(OffsetDateTime.parse(name, formatter)).toOption
  
  
  /**
   * Read a list of the prefixes, taking optional start time. Lowest precision available is 1 hour
   * @return A stream of root prefixes and the latest change date associated with them
   */
  extension (reader: BlobStorageReader[AdlsStoragePath]) def getRootPrefixes(storagePath: AdlsStoragePath, startFrom: OffsetDateTime): ZStream[Any, Throwable, (StoredBlob, String)] = for 
        _ <- zlogStream("Getting root prefixes starting from %s", startFrom.toString)
        // changelog.info indicates which batch is in progress right now - thus we remove it from eligible prefixes to avoid reading incomplete data
        inProgressDate <- ZStream.fromZIO(reader.readBlobContent(storagePath + "Changelog/changelog.info"))
        inProgressDateParsed <- ZStream.succeed(OffsetDateTime.parse(inProgressDate, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH.mm.ssX")))
        prefix <- ZStream.fromIterable(getPrefixesList(startFrom, inProgressDateParsed))
        eligibleBlob <- reader.streamPrefixes(storagePath + prefix)
          .map(blob => (interpretAsDate(blob), blob))
          .collect { 
            case (Some(date), blob) if (date.isAfter(startFrom) || date.isEqual(startFrom)) && (!date.isEqual(inProgressDateParsed)) => (blob, inProgressDate)
          }
  yield eligibleBlob

private def getPrefixesList(startDate: OffsetDateTime, endDate: OffsetDateTime): Seq[String] =
  val currentMoment = endDate.plusHours(1)
  val startMoment = startDate
  Iterator.iterate(startMoment)(_.plusHours(1))
    .takeWhile(_.toEpochSecond < currentMoment.toEpochSecond)
    .map { moment =>
      val monthString = s"00${moment.getMonth.getValue}".takeRight(2)
      val dayString = s"00${moment.getDayOfMonth}".takeRight(2)
      val hourString = s"00${moment.getHour}".takeRight(2)
      s"${moment.getYear}-$monthString-${dayString}T$hourString"
    }.to(LazyList)
