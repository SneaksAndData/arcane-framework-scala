package com.sneaksanddata.arcane.framework
package services.cdm

import logging.ZIOLogAnnotations.*
import models.cdm.given_Conversion_String_ArcaneSchema_DataRow
import models.{ArcaneSchema, DataRow}
import services.base.SchemaProvider
import services.cdm.BufferedReaderExtensions.streamMultilineCsv
import services.storage.base.BlobStorageReader
import services.storage.models.azure.AdlsStoragePath
import services.streaming.base.{MetadataEnrichedRowStreamElement, StreamDataProvider}

import zio.{ZIO, ZLayer}
import zio.stream.ZStream

import java.io.{BufferedReader, IOException}

type SynapseLinkStreamElement = DataRow | SourceCleanupRequest

given MetadataEnrichedRowStreamElement[SynapseLinkStreamElement] with
  extension (a: SynapseLinkStreamElement) def isDataRow: Boolean = a.isInstanceOf[DataRow]
  extension (a: SynapseLinkStreamElement) def toDataRow: DataRow = a.asInstanceOf[DataRow]
  extension (a: DataRow) def fromDataRow: SynapseLinkStreamElement = a

case class MetadataEnrichedReader(javaStream: BufferedReader, filePath: AdlsStoragePath, schemaProvider: SchemaProvider[ArcaneSchema])

case class SchemaEnrichedContent[TContent](content: TContent, schema: ArcaneSchema)

class MicrosoftSynapseLinkDataProvider(fileNameStreamSource: TableFilesStreamSource, reader: BlobStorageReader[AdlsStoragePath], rootPath: AdlsStoragePath) //(name: String, storagePath: AdlsStoragePath, azureBlogStorageReader: AzureBlobStorageReader, reader: AzureBlobStorageReader, streamContext: StreamContext)
  extends StreamDataProvider:

  override type StreamElementType = SynapseLinkStreamElement

  override def stream: ZStream[Any, Throwable, StreamElementType]  = fileNameStreamSource
    .lookBackStream
    .concat(fileNameStreamSource.changeCaptureStream)
    .mapZIO(openContentReader)
    .flatMap(readBlobContent)

  private def openContentReader(seb: SchemaEnrichedBlob): ZIO[Any, IOException, MetadataEnrichedReader] =
      reader.streamBlobContent(rootPath + seb.blob.name)
        .map(javaReader => MetadataEnrichedReader(javaReader, rootPath + seb.blob.name, seb.schemaProvider))
        .mapError(e => new IOException(s"Failed to get blob content: ${e.getMessage}", e))

  private def readBlobContent(mer: MetadataEnrichedReader): ZStream[Any, IOException, SynapseLinkStreamElement] =
    ZStream.acquireReleaseWith(ZIO.attempt(mer.javaStream))(javaStream => ZIO.succeed(javaStream.close()))
      .flatMap(javaReader => javaReader.streamMultilineCsv)
      .map(_.replace("\n", ""))
      .mapZIO(content => mer.schemaProvider.getSchema.map(schema => SchemaEnrichedContent(content, schema)))
      .mapZIO(sec => ZIO.attempt(implicitly[DataRow](sec.content, sec.schema)))
      .mapError(e => new IOException(s"Failed to parse CSV content: ${e.getMessage} from file: ${mer.filePath} with", e))
      .concat(ZStream.succeed(SourceCleanupRequest(mer.filePath)))
      .zipWithIndex
      .flatMap({
        case (e: SourceCleanupRequest, index: Long) => zlogStream(s"Received $index lines frm ${mer.filePath}, completed file I/O") *> ZStream.succeed(e)
        case (row: DataRow, _) => ZStream.succeed(row)
      })


object MicrosoftSynapseLinkDataProvider:
  def apply(fileNameStreamSource: TableFilesStreamSource, reader: BlobStorageReader[AdlsStoragePath], rootPath: AdlsStoragePath): MicrosoftSynapseLinkDataProvider =
    new MicrosoftSynapseLinkDataProvider(fileNameStreamSource, reader, rootPath)

  val layer: ZLayer[TableFilesStreamSource & BlobStorageReader[AdlsStoragePath] & AdlsStoragePath, Nothing, MicrosoftSynapseLinkDataProvider] =
    ZLayer {
      for
        context <- ZIO.service[TableFilesStreamSource]
        reader <- ZIO.service[BlobStorageReader[AdlsStoragePath]]
        rootPath <- ZIO.service[AdlsStoragePath]
      yield MicrosoftSynapseLinkDataProvider(context, reader, rootPath)
    }
