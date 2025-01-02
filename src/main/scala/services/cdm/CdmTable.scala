package com.sneaksanddata.arcane.framework
package services.cdm

import models.cdm.CSVParser.replaceQuotedNewlines
import models.cdm.{SimpleCdmEntity, given}
import models.{ArcaneSchema, DataRow}
import services.storage.models.azure.{AdlsStoragePath, AzureBlobStorageReader}

import java.time.{OffsetDateTime, ZoneOffset}
import scala.concurrent.Future

class CdmTable(name: String, storagePath: AdlsStoragePath, entityModel: SimpleCdmEntity, reader: AzureBlobStorageReader):
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  private val defaultFromYears: Int = 5
  private val schema: ArcaneSchema = implicitly(entityModel)

  /**
   * Read top-level virtual directories to allow pre-filtering blobs
   * @param startDate Baseline date to start search from
   * @return A list of yyyy-MM-ddTHH prefixes to apply as filters
   */
  private def getListPrefixes(startDate: Option[OffsetDateTime], endDate: Option[OffsetDateTime] = None): IndexedSeq[String] =
    val currentMoment = endDate.getOrElse(OffsetDateTime.now(ZoneOffset.UTC))
    val startMoment = startDate.getOrElse(currentMoment.minusYears(defaultFromYears))
    Iterator.iterate(startMoment)(_.plusHours(1))
      .takeWhile(_.toEpochSecond < currentMoment.toEpochSecond)
      .map { moment =>
        val monthString = s"00${moment.getMonth.getValue}".takeRight(2)
        val dayString = s"00${moment.getDayOfMonth}".takeRight(2)
        val hourString = s"00${moment.getHour}".takeRight(2)
        s"${moment.getYear}-$monthString-${dayString}T$hourString"
      }.toIndexedSeq

  /**
   * Read a table snapshot, taking optional start time. Lowest precision available is 1 hour
   * @param startDate Folders from Synapse export to include in the snapshot, based on the start date provided. If not provided, ALL folders from now - defaultFromYears will be included
   * @param endDate Date to stop at when looking for prefixes. In production use None for this value to always look data up to current moment.
   * @return A stream of rows for this table
   */
  def snapshot(startDate: Option[OffsetDateTime] = None, endDate: Option[OffsetDateTime] = None): Future[LazyList[DataRow]] =
    // list all matching blobs
    Future.sequence(getListPrefixes(startDate, endDate)
      .flatMap(prefix => reader.listPrefixes(storagePath + prefix))
      .flatMap(prefix => reader.listBlobs(storagePath + prefix.name + name))
      // exclude any files other than CSV
      .collect {
          case blob if blob.name.endsWith(".csv") => reader.getBlobContent(storagePath + blob.name)
      })
      .map(_.flatMap(content => replaceQuotedNewlines(content).split('\n').map(implicitly[DataRow](_, schema))))
      .map(LazyList.from)

object CdmTable:
  def apply(settings: CdmTableSettings, entityModel: SimpleCdmEntity, reader: AzureBlobStorageReader): CdmTable = new CdmTable(
    name = settings.name,
    storagePath = AdlsStoragePath(settings.rootPath).get,
    entityModel = entityModel,
    reader = reader
  )

