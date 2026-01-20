package com.sneaksanddata.arcane.framework
package services.synapse.base

import extensions.BufferedReaderExtensions.*
import logging.ZIOLogAnnotations.{zlog, zlogStream}
import models.cdm.given_Conversion_String_ArcaneSchema_DataRow
import models.schemas.ArcaneType.*
import models.schemas.{*, given}
import models.settings.SynapseSourceSettings
import services.base.SchemaProvider
import services.storage.models.azure.AdlsStoragePath
import services.storage.models.base.StoredBlob
import services.storage.services.azure.AzureBlobStorageReader
import services.synapse.SynapseAzureBlobReaderExtensions.*
import services.synapse.versioning.SynapseWatermark
import services.synapse.{SchemaEnrichedBlob, SchemaEnrichedContent, SynapseEntitySchemaProvider}

import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

import java.io.{BufferedReader, IOException}
import java.time.format.DateTimeFormatter
import java.time.*

final class SynapseLinkReader(entityName: String, storagePath: AdlsStoragePath, reader: AzureBlobStorageReader)
    extends SchemaProvider[ArcaneSchema]:

  /** Schema here comes from root-level model.json
    * @return
    *   A future containing the schema for the data produced by Arcane.
    */
  override def getSchema: Task[ArcaneSchema] =
    SynapseEntitySchemaProvider(reader, storagePath.toHdfsPath, entityName).getSchema

  override def empty: ArcaneSchema = ArcaneSchema.empty()

  /** Check if the provided candidate for a Synapse batch has a model.json file which contains batch schema.
    * @param batchFolderName
    * @return
    */
  private def hasSchemaFile(batchFolderName: String): Task[Boolean] =
    reader.blobExists(storagePath + batchFolderName + "model.json")

  /** Get files that belong to the current Synapse batch
    * @param batchFolderName
    * @return
    */
  private def getBatchFiles(batchFolderName: String): ZStream[Any, Throwable, StoredBlob] = reader
    .streamPrefixes(storagePath + batchFolderName + entityName + "/")
    .filter(sb => sb.name.endsWith(".csv"))

  private def enrichWithSchema(prefix: String): ZStream[Any, Throwable, SchemaEnrichedBlob] = {
    ZStream
      .fromZIO(for
        files <- getBatchFiles(prefix).runCollect // materialize CSV list to avoid double-querying storage
        _ <- zlog(
          "Found %s CSV files with changes for entity %s at batch folder %s",
          files.size.toString,
          entityName,
          prefix
        )
        dataBlobs <- ZIO.when(files.nonEmpty) {
          for
            // getSchema here performs runtime check for model.json for the batch to be parseable and readable
            // this will hard fail compared to `hasSchemaFile` just filtering invalid batches out.
            // this allows to separate batch filtering for eligibility for processing, from batch data correctness checks
            orderedFiles <- SynapseEntitySchemaProvider(
              reader,
              (storagePath + prefix).toHdfsPath,
              entityName
            ).getSchema.map(schema =>
              files
                // we need to emit deletions, which are in files named 1.csv, last
                // otherwise for batches where deletions come alongside insertions there is a risk of running a delete BEFORE the insert
                .sortBy(b => b.name.split("/").last.replace(".csv", "").toInt)(Ordering.Int.reverse)
                .map(csvBlob => SchemaEnrichedBlob(csvBlob, schema))
            )
          yield orderedFiles
        }
      yield dataBlobs)
      .flatMap {
        case Some(files) =>
          zlogStream("Starting stream of the following: %s", files.map(_.blob.name).mkString(",")) *> ZStream
            .fromIterable(files)
        case None =>
          zlogStream("Batch %s has no changes for the entity %s", prefix, entityName) *> ZStream.empty
      }
  }

  /** Select ALL CSV files that correspond to the entity changes
    *
    * Hierarchical listing: First get entity folders under each date folder Select folder matching our entity List that
    * folder for CSV files
    *
    * @return
    *   A stream of rows for this table
    */
  private def getEntityChangeData(version: SynapseWatermark): ZStream[Any, Throwable, SchemaEnrichedBlob] =
    enrichWithSchema(version.prefix)

  private def getFileStream(
      seb: SchemaEnrichedBlob
  ): ZIO[Any, IOException, (BufferedReader, ArcaneSchema, StoredBlob)] =
    reader
      .streamBlobContent(storagePath + seb.blob.name)
      .map(javaReader => (javaReader, seb.schema, seb.blob))
      .mapError(e => new IOException(s"Failed to get blob content: ${e.getMessage}", e))

  private def getTableChanges(
      fileStream: BufferedReader,
      fileSchema: ArcaneSchema,
      fileName: String
  ): ZStream[Any, IOException, DataRow] =
    ZStream
      .acquireReleaseWith(ZIO.attemptBlockingIO(fileStream))(stream => ZIO.succeed(stream.close()))
      .flatMap(javaReader => javaReader.streamMultilineCsv)
      .map(_.replace("\n", ""))
      .map(content => SchemaEnrichedContent(content, fileSchema))
      .mapZIO(sec => ZIO.attempt(implicitly[DataRow](sec.content, sec.schema)))
      .mapError(e => new IOException(s"Failed to parse CSV content: ${e.getMessage} from file: $fileName with", e))

  private def isValidSynapseBatch(prefix: String): ZIO[Any, Throwable, Boolean] = hasSchemaFile(prefix)

  /** Get the latest batch folder that is ready for streaming
    * @param previousVersion
    *   Previous valid batch folder
    * @return
    */
  def getCurrentVersion(previousVersion: SynapseWatermark): Task[SynapseWatermark] =
    for synapseBlob <- reader.getCurrentBatch(storagePath).map {
        // in case of a read failure, fallback to previous version - should never happen, but framework expects this method to always succeed
        case version if version.interpretAsDate.isDefined => Some(version)
        case _                                            => None
      }
    yield synapseBlob.map(_.asWatermark).getOrElse(previousVersion)

  /** TODO: temporary method that will be removed once watermarking is implemented. Should be replaced with watermark
    * read and fallback to reading all container in case watermark is not found
    * @param startFrom
    * @return
    */
  def getVersion(startFrom: OffsetDateTime): Task[SynapseWatermark] =
    for
      candidates <- reader.getEligibleDates(storagePath, startFrom).runCollect
      allCandidates <- ZIO.when(candidates.isEmpty) {
        for all <- reader
            .getEligibleDates(storagePath, OffsetDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC))
            .runCollect
        yield all
      }
    yield allCandidates.getOrElse(candidates).minBy(_.asDate).asWatermark

  /** Check if the provided batch folder has relevant changes - only take a batch that has model.json committed
    * @param latestVersion
    *   Watermark to check for changes
    * @return
    */
  def hasChanges(latestVersion: SynapseWatermark): Task[Boolean] = isValidSynapseBatch(latestVersion.prefix)

  // TODO: when watermark comparison is added, getEligibleDates can be skipped if diff(prev, current) <= changeTrackingInterval * 1.5
  /** Reads changes happened since startFrom date. Inserts and updates are always emitted first, to avoid re-inserting
    * deleted records. Start date to get changes from
    * @return
    */
  def getChanges(version: SynapseWatermark): ZStream[Any, Throwable, DataRow] = reader
    .getEligibleDates(storagePath = storagePath, startFrom = version.timestamp)
    .map(_.asWatermark)
    .flatMap(getChangesForVersion)

  /** Reads changes happened since startFrom date. Inserts and updates are always emitted first, to avoid re-inserting
    * deleted records. Start date to get changes from
    *
    * @return
    */
  private def getChangesForVersion(version: SynapseWatermark): ZStream[Any, Throwable, DataRow] =
    getEntityChangeData(version)
      .mapZIO(getFileStream)
      .flatMap { case (fileStream, fileSchema, blob) =>
        getTableChanges(fileStream, fileSchema, blob.name)
      }
      .map(convertRow)

  def getData(startFrom: OffsetDateTime): ZStream[Any, Throwable, DataRow] = reader
    .getEligibleDates(storagePath, startFrom)
    .map(_.asWatermark)
    .filterZIO(wm => isValidSynapseBatch(wm.prefix))
    .flatMap(getChangesForVersion)

  /** Row type conversions. Should be moved to a separate class, implementing IcebergRowConverter trait, see
    * https://github.com/SneaksAndData/arcane-framework-scala/issues/125
    */

  private def convertRow(row: DataRow): DataRow = row.map(convertCell)

  private def convertCell(cell: DataCell): DataCell =
    cell.value match
      case None    => cell.copy(name = cell.name, Type = cell.Type, value = null)
      case Some(v) => cell.copy(name = cell.name, Type = cell.Type, value = valueAsJava(cell.name, cell.Type, v))

  private def valueAsJava(fieldName: String, arcaneType: ArcaneType, value: Any): Any = arcaneType match
    case LongType             => value.toString.toLong
    case ByteArrayType        => value.toString.getBytes
    case BooleanType          => value.toString.toBoolean
    case StringType           => value.toString
    case DateType             => java.sql.Date.valueOf(value.toString)
    case TimestampType        => valueAsTimeStamp(fieldName, value)
    case DateTimeOffsetType   => valueAsOffsetDateTime(value)
    case BigDecimalType(_, _) => BigDecimal(value.toString)
    case DoubleType           => value.toString.toDouble
    case IntType              => value.toString.toInt
    case FloatType            => value.toString.toFloat
    case ShortType            => value.toString.toShort
    case TimeType             => java.sql.Time.valueOf(value.toString)
    case ListType(_, _)       => throw new UnsupportedOperationException(s"Unsupported List type for field $fieldName")
    case ObjectType    => throw new UnsupportedOperationException(s"Unsupported Object type for field $fieldName")
    case StructType(_) => throw new UnsupportedOperationException(s"Unsupported Struct type for field $fieldName")

  private def valueAsOffsetDateTime(value: Any): OffsetDateTime = value match
    case timestampValue: String if timestampValue.endsWith("Z") => OffsetDateTime.parse(timestampValue)
    case timestampValue: String if timestampValue.contains("+00:00") =>
      OffsetDateTime.parse(timestampValue, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSXXX"))
    case timestampValue: String =>
      LocalDateTime
        .parse(timestampValue, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSS"))
        .atOffset(ZoneOffset.UTC)
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
          LocalDateTime.ofInstant(
            OffsetDateTime.parse(timestampValue, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant,
            ZoneId.systemDefault()
          )
        case _ =>
          // format  from MS docs: yyyy-MM-dd'T'HH:mm:ss'Z'
          // example from MS docs: 2021-06-25T16:21:12Z
          // this will parse: 2021-06-25T16:21:12Z, 2021-06-25T16:21:12, 2021-06-25T16:21:12.1231
          if (timestampValue.endsWith("Z"))
            LocalDateTime.parse(timestampValue, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
          else
            LocalDateTime.parse(timestampValue, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    case _ => throw new IllegalArgumentException(s"Invalid timestamp type: ${value.getClass}")

object SynapseLinkReader:
  def apply(blobStorageReader: AzureBlobStorageReader, name: String, location: AdlsStoragePath): SynapseLinkReader =
    new SynapseLinkReader(name, location, blobStorageReader)

  val layer: ZLayer[SynapseSourceSettings & AzureBlobStorageReader, IllegalArgumentException, SynapseLinkReader] =
    ZLayer {
      for
        blobReader     <- ZIO.service[AzureBlobStorageReader]
        sourceSettings <- ZIO.service[SynapseSourceSettings]
        adlsLocation <- ZIO.getOrFailWith(new IllegalArgumentException("Invalid ADLSGen2 path provided"))(
          AdlsStoragePath(sourceSettings.baseLocation).toOption
        )
      yield SynapseLinkReader(blobReader, sourceSettings.entityName, adlsLocation)
    }
