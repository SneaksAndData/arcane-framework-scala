package com.sneaksanddata.arcane.framework
package services.synapse.base

import models.app.StreamContext
import models.settings.{SynapseSourceSettings, TargetTableSettings}
import services.storage.models.azure.AdlsStoragePath
import services.base.{SchemaProvider, TableManager}
import services.storage.services.AzureBlobStorageReader
import services.storage.models.base.StoredBlob
import services.synapse.SynapseAzureBlobReaderExtensions.*
import services.base.BufferedReaderExtensions.*
import models.{ArcaneSchema, ArcaneType, DataCell, DataRow, given_CanAdd_ArcaneSchema}
import services.synapse.{SchemaEnrichedBlob, SchemaEnrichedContent, SynapseEntitySchemaProvider}
import models.cdm.given_Conversion_String_ArcaneSchema_DataRow
import logging.ZIOLogAnnotations.zlogStream

import com.sneaksanddata.arcane.framework.models.ArcaneType.*
import zio.{Task, ZIO, ZLayer}
import zio.stream.{ZPipeline, ZStream}

import java.io.{BufferedReader, IOException}
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, OffsetDateTime, ZoneId, ZoneOffset}

final class SynapseLinkReader(entityName: String, storagePath: AdlsStoragePath, reader: AzureBlobStorageReader) extends SchemaProvider[ArcaneSchema]:

  /**
   * Schema here comes from root-level model.json
   *  @return A future containing the schema for the data produced by Arcane.
   */
  override def getSchema: Task[ArcaneSchema] = SynapseEntitySchemaProvider(reader, storagePath.toHdfsPath, entityName)
    .getSchema

  override def empty: ArcaneSchema = ArcaneSchema.empty()
  
  def getLatestVersion: Task[String] = reader.getInProgressVersion(storagePath)

  private def enrichWithSchema(stream: ZStream[Any, Throwable, (StoredBlob, String)]): ZStream[Any, Throwable, SchemaEnrichedBlob] =
    stream
      .filterZIO(prefix => reader.blobExists(storagePath + prefix._1.name + "model.json"))
      .mapZIO { prefix =>
        SynapseEntitySchemaProvider(reader, (storagePath + prefix._1.name).toHdfsPath, entityName)
          .getSchema
          .map(schema => SchemaEnrichedBlob(prefix._1, schema, prefix._2))
      }

  private def filterBlobs(endsWithString: String, blobStream: ZStream[Any, Throwable, SchemaEnrichedBlob]) = blobStream
    .flatMap(seb => reader.streamPrefixes(storagePath + seb.blob.name).map(sb => SchemaEnrichedBlob(sb, seb.schema, seb.latestVersion)))
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
      s"/$entityName/", enrichWithSchema(reader.getRootPrefixes(storagePath, startDate))
    )
  )

  private def getFileStream(seb: SchemaEnrichedBlob): ZIO[Any, IOException, (BufferedReader, ArcaneSchema, StoredBlob, String)] =
    reader.streamBlobContent(storagePath + seb.blob.name)
      .map(javaReader => (javaReader, seb.schema, seb.blob, seb.latestVersion))
      .mapError(e => new IOException(s"Failed to get blob content: ${e.getMessage}", e))

  private def getTableChanges(fileStream: BufferedReader, fileSchema: ArcaneSchema, fileName: String, latestVersion: String): ZStream[Any, IOException, (DataRow, String)] =
    ZStream.acquireReleaseWith(ZIO.attemptBlockingIO(fileStream))(stream => ZIO.succeed(stream.close()))
      .flatMap(javaReader => javaReader.streamMultilineCsv)
      .map(_.replace("\n", ""))
      .map(content => SchemaEnrichedContent(content, fileSchema, latestVersion))
      .mapZIO(sec => ZIO.attempt((implicitly[DataRow](sec.content, sec.schema), sec.latestVersion)))
      .mapError(e => new IOException(s"Failed to parse CSV content: ${e.getMessage} from file: $fileName with", e))


  /**
   * Reads changes happened since startFrom date
   * @param startFrom Start date to get changes from
   * @return
   */
  def getChanges(startFrom: OffsetDateTime): ZStream[Any, Throwable, (DataRow, String)] = getEntityChangeData(startFrom)
    .mapZIO(getFileStream)
    .flatMap {
      case (fileStream, fileSchema, blob, latestVersion) => getTableChanges(fileStream, fileSchema, blob.name, latestVersion)
    }.map {
      case (row, version) => (convertRow(row), version)
    }

/**
 * Row type conversions. Should be moved to a separate class, implementing IcebergRowConverter trait, see
 * https://github.com/SneaksAndData/arcane-framework-scala/issues/125
 */

  private def convertRow(row: DataRow): DataRow = row.map(convertCell)

  private def convertCell(cell: DataCell): DataCell =
    cell.value match
      case None => cell.copy(name = cell.name, Type = cell.Type, value = null)
      case Some(v) => cell.copy(name = cell.name, Type = cell.Type, value = valueAsJava(cell.name, cell.Type, v))

  private def valueAsJava(fieldName: String, arcaneType: ArcaneType, value: Any): Any = arcaneType match
    case LongType => value.toString.toLong
    case ByteArrayType => value.toString.getBytes
    case BooleanType => value.toString.toBoolean
    case StringType => value.toString
    case DateType => java.sql.Date.valueOf(value.toString)
    case TimestampType => valueAsTimeStamp(fieldName, value)
    case DateTimeOffsetType => valueAsOffsetDateTime(value)
    case BigDecimalType => BigDecimal(value.toString)
    case DoubleType => value.toString.toDouble
    case IntType => value.toString.toInt
    case FloatType => value.toString.toFloat
    case ShortType => value.toString.toShort
    case TimeType => java.sql.Time.valueOf(value.toString)

  private def valueAsOffsetDateTime(value: Any): OffsetDateTime = value match
    case timestampValue: String if timestampValue.endsWith("Z")
    => OffsetDateTime.parse(timestampValue)
    case timestampValue: String if timestampValue.contains("+00:00")
    => OffsetDateTime.parse(timestampValue, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSXXX"))
    case timestampValue: String
    => LocalDateTime.parse(timestampValue, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSS")).atOffset(ZoneOffset.UTC)
    case _ => throw new IllegalArgumentException(s"Invalid timestamp type: ${value.getClass}")

  private def valueAsTimeStamp(columnName: String, value: Any): LocalDateTime = value match
    case timestampValue: String =>
      columnName match
        case "SinkCreatedOn" | "SinkModifiedOn" =>
          // format  from MS docs: M/d/yyyy H:mm:ss tt
          // example from MS docs: 6/28/2021 4:34:35 PM
          LocalDateTime.parse(timestampValue, DateTimeFormatter.ofPattern("M/d/yyyy h:mm:ss a"))
        case "CreatedOn" =>
          // format  from MS docs: yyyy-MM-dd'T'HH:mm:ss.sssssssXXX
          // example from MS docs: 2018-05-25T16:21:09.0000000+00:00
          LocalDateTime.ofInstant(OffsetDateTime.parse(timestampValue, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant,
            ZoneId.systemDefault())
        case _ =>
          // format  from MS docs: yyyy-MM-dd'T'HH:mm:ss'Z'
          // example from MS docs: 2021-06-25T16:21:12Z
          val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSS");
          if (timestampValue.endsWith("Z")) {
            LocalDateTime.parse(timestampValue, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
          } else {
            LocalDateTime.parse(timestampValue, formatter)
          }
    case _ => throw new IllegalArgumentException(s"Invalid timestamp type: ${value.getClass}")

object SynapseLinkReader:
  def apply(blobStorageReader: AzureBlobStorageReader, name: String, location: AdlsStoragePath): SynapseLinkReader =
    new SynapseLinkReader(name, location, blobStorageReader)

  val layer: ZLayer[SynapseSourceSettings & AzureBlobStorageReader, IllegalArgumentException, SynapseLinkReader] = ZLayer {
    for
      blobReader <- ZIO.service[AzureBlobStorageReader]
      sourceSettings <- ZIO.service[SynapseSourceSettings]
      adlsLocation <- ZIO.getOrFailWith(new IllegalArgumentException("Invalid ADLSGen2 path provided"))(AdlsStoragePath(sourceSettings.baseLocation).toOption)
    yield SynapseLinkReader(blobReader, sourceSettings.entityName, adlsLocation)
  }
