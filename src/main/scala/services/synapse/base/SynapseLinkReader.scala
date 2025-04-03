package com.sneaksanddata.arcane.framework
package services.synapse.base

import models.app.StreamContext
import models.settings.TargetTableSettings
import services.storage.models.azure.AdlsStoragePath
import services.base.TableManager
import services.storage.services.AzureBlobStorageReader
import services.storage.models.base.StoredBlob
import services.synapse.SynapseAzureBlobReaderExtensions.*
import services.base.BufferedReaderExtensions.*
import models.{ArcaneSchema, DataRow}
import services.synapse.{SchemaEnrichedBlob, SchemaEnrichedContent, SynapseEntitySchemaProvider}
import models.cdm.given_Conversion_String_ArcaneSchema_DataRow
import logging.ZIOLogAnnotations.zlogStream

import zio.ZIO
import zio.stream.ZStream

import java.io.{BufferedReader, IOException}
import java.time.{OffsetDateTime, ZoneOffset}

final class SynapseLinkReader(entityName: String, storagePath: AdlsStoragePath, azureBlogStorageReader: AzureBlobStorageReader, reader: AzureBlobStorageReader):

  private def enrichWithSchema(stream: ZStream[Any, Throwable, StoredBlob]): ZStream[Any, Throwable, SchemaEnrichedBlob] =
    stream
      .filterZIO(prefix => azureBlogStorageReader.blobExists(storagePath + prefix.name + "model.json"))
      .mapZIO { prefix =>
        SynapseEntitySchemaProvider(reader, (storagePath + prefix.name).toHdfsPath, entityName)
          .getSchema
          .map(schema => SchemaEnrichedBlob(prefix, schema))
      }

  /**
   * Read a table snapshot, taking optional start time. Lowest precision available is 1 hour
   *
   * @return A stream of rows for this table
   */
  private def getEntityChangeData(startDate: OffsetDateTime): ZStream[Any, Throwable, SchemaEnrichedBlob] = enrichWithSchema(azureBlogStorageReader.getRootPrefixes(storagePath, startDate))
      // hierarchical listing:
      // first get entity folders under each date folder
      // select folder matching our entity
      // list that folder for CSV files
      .flatMap(seb => azureBlogStorageReader.streamPrefixes(storagePath + seb.blob.name).map(sb => SchemaEnrichedBlob(sb, seb.schema)))
      .filter(seb => seb.blob.name.endsWith(s"/$entityName/"))
      .flatMap(seb => azureBlogStorageReader.streamPrefixes(storagePath + seb.blob.name).map(sb => SchemaEnrichedBlob(sb, seb.schema)))
      .filter(seb => seb.blob.name.endsWith(".csv"))


//  /**
//   * Read a table snapshot, taking optional start time. Lowest precision available is 1 hour
//   *
//   * @param lookBackInterval      The look back interval to start from
//   * @param changeCaptureInterval Interval to capture changes
//   * @return A stream of rows for this table
//   */
//  def snapshotPrefixes(lookBackInterval: Duration, changeCaptureInterval: Duration, changeCapturePeriod: Duration): ZStream[Any, Throwable, SchemaEnrichedBlob] =
//    val initialPrefixes = getRootDropPrefixes(storagePath, lookBackInterval).flatMap(s => s.runCollect)
//    // data from lookback
//    val firstStream = ZStream.fromZIO(initialPrefixes)
//      .flatMap(x => ZStream.fromIterable(x))
//      .flatMap(seb => azureBlogStorageReader.streamPrefixes(storagePath + seb.blob.name).withSchema(seb.schemaProvider))
//      .filter(seb => seb.blob.name.endsWith(s"/$name/"))
//      .flatMap(seb => azureBlogStorageReader.streamPrefixes(storagePath + seb.blob.name).withSchema(seb.schemaProvider))
//      .filter(seb => seb.blob.name.endsWith(".csv"))
//
//    // iterative change capture
//    // every `changeCaptureInterval` seconds we read timestamp from Changelog/changelog.info file and subtract 2*changeCaptureInterval from it
//    val repeatStream = ZStream.fromZIO(dropLast(getRootDropPrefixes(storagePath, changeCapturePeriod)))
//      .flatMap(x => ZStream.fromIterable(x))
//      .flatMap(seb => azureBlogStorageReader.streamPrefixes(storagePath + seb.blob.name).withSchema(seb.schemaProvider))
//      .filter(seb => seb.blob.name.endsWith(s"/$name/"))
//      .flatMap(seb => azureBlogStorageReader.streamPrefixes(storagePath + seb.blob.name).withSchema(seb.schemaProvider))
//      .filter(seb => seb.blob.name.endsWith(".csv"))
//      .repeat(Schedule.spaced(changeCaptureInterval))
//
//    firstStream.concat(repeatStream)

  private def getFileStream(seb: SchemaEnrichedBlob): ZIO[Any, IOException, (BufferedReader, ArcaneSchema, StoredBlob)] =
    azureBlogStorageReader.streamBlobContent(storagePath + seb.blob.name)
      .map(javaReader => (javaReader, seb.schema, seb.blob))
      .mapError(e => new IOException(s"Failed to get blob content: ${e.getMessage}", e))

  private def getTableChanges(fileStream: BufferedReader, fileSchema: ArcaneSchema, fileName: String): ZStream[Any, IOException, DataRow] =
    ZStream.acquireReleaseWith(ZIO.attemptBlockingIO(fileStream))(stream => ZIO.succeed(stream.close()))
      .flatMap(javaReader => javaReader.streamMultilineCsv)
      .map(_.replace("\n", ""))
      .map(content => SchemaEnrichedContent(content, fileSchema))
      .mapZIO(sec => ZIO.attempt(implicitly[DataRow](sec.content, sec.schema)))
      .mapError(e => new IOException(s"Failed to parse CSV content: ${e.getMessage} from file: ${fileName} with", e))


  /**
   * Reads changes happened since startFrom date
   * @param startFrom Start date to get changes from
   * @return
   */
  def getChanges(startFrom: OffsetDateTime): ZStream[Any, Throwable, (DataRow, String)] = getEntityChangeData(startFrom)
    .mapZIO(getFileStream)
    .flatMap {
      case (fileStream, fileSchema, blob) => getTableChanges(fileStream, fileSchema, blob.name).map(row => (row, blob.name.split("/").head))
    }
