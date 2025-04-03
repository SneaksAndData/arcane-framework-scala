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
import zio.stream.{ZPipeline, ZStream}

import java.io.{BufferedReader, IOException}
import java.time.{OffsetDateTime, ZoneOffset}

final class SynapseLinkReader(entityName: String, storagePath: AdlsStoragePath, azureBlogStorageReader: AzureBlobStorageReader, reader: AzureBlobStorageReader):

  private def enrichWithSchema(stream: ZStream[Any, Throwable, (StoredBlob, String)]): ZStream[Any, Throwable, SchemaEnrichedBlob] =
    stream
      .filterZIO(prefix => azureBlogStorageReader.blobExists(storagePath + prefix._1.name + "model.json"))
      .mapZIO { prefix =>
        SynapseEntitySchemaProvider(reader, (storagePath + prefix._1.name).toHdfsPath, entityName)
          .getSchema
          .map(schema => SchemaEnrichedBlob(prefix._1, schema, prefix._2))
      }

  private def filterBlobs(endsWithString: String, blobStream: ZStream[Any, Throwable, SchemaEnrichedBlob]) = blobStream
    .flatMap(seb => azureBlogStorageReader.streamPrefixes(storagePath + seb.blob.name).map(sb => SchemaEnrichedBlob(sb, seb.schema, seb.latestVersion)))
    .filter(seb => seb.blob.name.endsWith(endsWithString))

  /**
   * Read a table snapshot, taking optional start time. Lowest precision available is 1 hour
   *
   * Hierarchical listing:
   * First get entity folders under each date folder
   * Select folder matching our entity
   * List that folder for CSV files
   *
   * @return A stream of rows for this table
   */
  private def getEntityChangeData(startDate: OffsetDateTime): ZStream[Any, Throwable, SchemaEnrichedBlob] = filterBlobs(
    ".csv", 
    filterBlobs(
      s"/$entityName/", enrichWithSchema(azureBlogStorageReader.getRootPrefixes(storagePath, startDate))
    )
  )

  private def getFileStream(seb: SchemaEnrichedBlob): ZIO[Any, IOException, (BufferedReader, ArcaneSchema, StoredBlob, String)] =
    azureBlogStorageReader.streamBlobContent(storagePath + seb.blob.name)
      .map(javaReader => (javaReader, seb.schema, seb.blob, seb.latestVersion))
      .mapError(e => new IOException(s"Failed to get blob content: ${e.getMessage}", e))

  private def getTableChanges(fileStream: BufferedReader, fileSchema: ArcaneSchema, fileName: String, latestVersion: String): ZStream[Any, IOException, (DataRow, String)] =
    ZStream.acquireReleaseWith(ZIO.attemptBlockingIO(fileStream))(stream => ZIO.succeed(stream.close()))
      .flatMap(javaReader => javaReader.streamMultilineCsv)
      .map(_.replace("\n", ""))
      .map(content => SchemaEnrichedContent(content, fileSchema, latestVersion))
      .mapZIO(sec => ZIO.attempt((implicitly[DataRow](sec.content, sec.schema), sec.latestVersion)))
      .mapError(e => new IOException(s"Failed to parse CSV content: ${e.getMessage} from file: ${fileName} with", e))


  /**
   * Reads changes happened since startFrom date
   * @param startFrom Start date to get changes from
   * @return
   */
  def getChanges(startFrom: OffsetDateTime): ZStream[Any, Throwable, (DataRow, String)] = getEntityChangeData(startFrom)
    .mapZIO(getFileStream)
    .flatMap {
      case (fileStream, fileSchema, blob, latestVersion) => getTableChanges(fileStream, fileSchema, blob.name, latestVersion)
    }
