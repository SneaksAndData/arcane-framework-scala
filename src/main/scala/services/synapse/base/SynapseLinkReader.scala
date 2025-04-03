package com.sneaksanddata.arcane.framework
package services.synapse.base

import models.app.StreamContext
import models.settings.TargetTableSettings

import services.storage.models.azure.AdlsStoragePath
import services.base.TableManager
import services.storage.services.AzureBlobStorageReader
import services.cdm.SchemaEnrichedBlob
import services.storage.models.base.StoredBlob
import services.synapse.SynapseAzureBlobReaderExtensions._
import com.sneaksanddata.arcane.framework.services.synapse.SynapseEntitySchemaProvider

import zio.ZIO
import zio.stream.ZStream

import java.time.{OffsetDateTime, ZoneOffset}

class SynapseLinkReader(entityName: String, storagePath: AdlsStoragePath, azureBlogStorageReader: AzureBlobStorageReader, reader: AzureBlobStorageReader):

  private def enrichWithSchema(stream: ZStream[Any, Throwable, StoredBlob]): ZStream[Any, Throwable, SchemaEnrichedBlob] =
    stream
      .filterZIO(prefix => azureBlogStorageReader.blobExists(storagePath + prefix.name + "model.json"))
      .mapZIO { prefix =>
        SynapseEntitySchemaProvider(reader, (storagePath + prefix.name).toHdfsPath, entityName, None)
          .getSchema
          .map(schema => SchemaEnrichedBlob(prefix, schema))
      }

  /**
   * Read a table snapshot, taking optional start time. Lowest precision available is 1 hour
   *
   * @return A stream of rows for this table
   */
  def getEntityChangeData(startDate: OffsetDateTime): ZStream[Any, Throwable, SchemaEnrichedBlob] = enrichWithSchema(azureBlogStorageReader.getRootPrefixes(storagePath, startDate))
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

//  def getStream(seb: SchemaEnrichedBlob): ZIO[Any, IOException, MetadataEnrichedReader] =
//    azureBlogStorageReader.streamBlobContent(storagePath + seb.blob.name)
//      .map(javaReader => MetadataEnrichedReader(javaReader, storagePath + seb.blob.name, seb.schemaProvider))
//      .mapError(e => new IOException(s"Failed to get blob content: ${e.getMessage}", e))

  def tryGetContinuation(stream: BufferedReader, quotes: Int, accum: StringBuilder): ZIO[Any, Throwable, String] =
    if quotes % 2 == 0 then
      ZIO.succeed(accum.toString())
    else
      for {
        line <- ZIO.attemptBlocking(Option(stream.readLine()))
        continuation <- tryGetContinuation(stream, quotes + line.getOrElse("").count(_ == '"'), accum.append(line.map(l => s"\n$l").getOrElse("")))
      }
      yield continuation

  def getLine(stream: BufferedReader): ZIO[Any, Throwable, Option[String]] =
    for {
      dataLine <- ZIO.attemptBlocking(Option(stream.readLine()))
      continuation <- tryGetContinuation(stream, dataLine.getOrElse("").count(_ == '"'), new StringBuilder())
    }
    yield {
      dataLine match
        case None => None
        case Some(dataLine) if dataLine == "" => None
        case Some(dataLine) => Some(s"$dataLine\n$continuation")
    }

  def getData(streamData: MetadataEnrichedReader): ZStream[Any, IOException, DataStreamElement] =
    ZStream.acquireReleaseWith(ZIO.attempt(streamData.javaStream))(stream => ZIO.succeed(stream.close()))
      .flatMap(javaReader => ZStream.repeatZIO(getLine(javaReader)))
      .takeWhile(_.isDefined)
      .map(_.get)
      .map(_.replace("\n", ""))
      .mapZIO(content => streamData.schemaProvider.await.map(schema => SchemaEnrichedContent(content, schema)))
      .mapZIO(sec => ZIO.attempt(implicitly[DataRow](sec.content, sec.schema)))
      .mapError(e => new IOException(s"Failed to parse CSV content: ${e.getMessage} from file: ${streamData.filePath} with", e))
      .concat(ZStream.succeed(SourceCleanupRequest(streamData.filePath)))
      .zipWithIndex
      .flatMap({
        case (e: SourceCleanupRequest, index: Long) => zlogStream(s"Received $index lines frm ${streamData.filePath}, completed file I/O") *> ZStream.succeed(e)
        case (r: DataRow, _) => ZStream.succeed(r)
      })

