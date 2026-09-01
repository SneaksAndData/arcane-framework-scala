package com.sneaksanddata.arcane.framework
package services.synapse.base

import extensions.BufferedReaderExtensions.*
import logging.ZIOLogAnnotations.{zlog, zlogStream}
import models.app.PluginStreamContext
import models.schemas.ArcaneType.*
import models.schemas.{*, given}
import models.settings.sources.synapse.MicrosoftSynapseLinkConnectionSettings
import models.settings.{AllFieldsImpl, ExcludeFieldsImpl, FieldSelectionRuleSettings, IncludeFieldsImpl}
import models.settings.sources.modification.ConfigurableDataRowModification
import models.cdm.CSVParser
import services.base.{DefaultStreamingSource, SchemaProvider}
import services.storage.models.azure.AdlsStoragePath
import services.storage.models.base.StoredBlob
import services.storage.services.azure.AzureBlobStorageReader
import services.streaming.base.StructuredZStream
import services.synapse.SynapseAzureBlobReaderExtensions.*
import services.synapse.versioning.SynapseWatermark
import services.synapse.SynapseEntitySchemaProvider

import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

import java.io.{BufferedReader, IOException}
import java.time.*
import java.time.format.DateTimeFormatter

final class SynapseLinkStreamingSource(
    location: AdlsStoragePath,
    entityName: String,
    reader: AzureBlobStorageReader,
    fieldSelector: FieldSelectionRuleSettings,
    modifications: Seq[ConfigurableDataRowModification]
) extends DefaultStreamingSource(modifications):

  override type ShardMetadata = (stream: StructuredZStream, source: String)
  override type WatermarkType = SynapseWatermark

  override protected val primaryKeyNames: Task[Seq[String]] = ZIO.succeed(Seq("Id"))

  override protected val versionName: Task[String] = ZIO.succeed("versionnumber")

  // in 2.4 release this will be integrated via DataRowModification and provided uniformly for all source
  // this code only addresses schema alignment issues in 2.3 release for non-server-side filtered sources.
  private def applyFieldSelector(schema: ArcaneSchema): ArcaneSchema =
    fieldSelector.rule match
      case AllFieldsImpl(_) => schema
      case IncludeFieldsImpl(includeFields) =>
        schema.filter(f =>
          includeFields.fields.exists(_.equalsIgnoreCase(f.name)) || fieldSelector.essentialFields.exists(
            _.equalsIgnoreCase(f.name)
          )
        )
      case ExcludeFieldsImpl(excludeFields) =>
        schema.filterNot(f => excludeFields.fields.exists(_.equalsIgnoreCase(f.name)))

  private def applyFieldSelector(row: DataRow): DataRow =
    fieldSelector.rule match
      case AllFieldsImpl(_) => row
      case IncludeFieldsImpl(includeFields) =>
        row.filter(cell =>
          includeFields.fields.exists(_.equalsIgnoreCase(cell.name)) || fieldSelector.essentialFields.exists(
            _.equalsIgnoreCase(cell.name)
          )
        )
      case ExcludeFieldsImpl(excludeFields) =>
        row.filterNot(cell => excludeFields.fields.exists(_.equalsIgnoreCase(cell.name)))

  /** Schema here comes from root-level model.json
    */
  override protected def getSourceSchema: Task[ArcaneSchema] =
    SynapseEntitySchemaProvider(reader, location.toHdfsPath, entityName).getSchema
      .map(applyFieldSelector)

  /** Schema from batch-level model.json
    */
  private def getBatchSchema(batchFolderName: String): Task[ArcaneSchema] =
    SynapseEntitySchemaProvider(reader, (location + batchFolderName).toHdfsPath, entityName).getSchema
      .map(applyFieldSelector)

  override def empty: ArcaneSchema = ArcaneSchema.empty()

  /** Check if the provided candidate for a Synapse batch has a model.json file which contains batch schema.
    * @return
    */
  private def hasSchemaFile(batchFolderName: String): Task[Boolean] =
    reader.blobExists(location + batchFolderName + "model.json")

  /** Get files that belong to the current Synapse batch
    * @return
    */
  private def getBatchFiles(batchFolderName: String): ZStream[Any, Throwable, StoredBlob] = reader
    .streamPrefixes(location + batchFolderName + entityName + "/")
    .filter(sb => sb.name.endsWith(".csv"))

  private def fetchSortedFiles(prefix: String): ZStream[Any, Throwable, StoredBlob] =
    ZStream
      .fromZIO {
        for
          files <- getBatchFiles(prefix).runCollect
          _ <- zlog(
            "Found %s CSV files with changes for entity %s at batch folder %s",
            files.size.toString,
            entityName,
            prefix
          )
        yield files
      }
      .flatMap { files =>
        if files.nonEmpty then
          val sortedFiles =
            files
              // we need to emit deletions, which are in files named 1.csv, last
              // otherwise for batches where deletions come alongside insertions there is a risk of running a delete BEFORE the insert
              .sortBy(
                _.name.split("/").last.replace(".csv", "").toInt
              )(using Ordering.Int.reverse)
          zlogStream(
            "Starting stream of the following: %s",
            sortedFiles.map(_.name).mkString(",")
          ) *> ZStream.fromIterable(sortedFiles)
        else
          zlogStream(
            "Batch %s has no changes for the entity %s",
            prefix,
            entityName
          ) *> ZStream.empty
      }

  /** Select ALL CSV files that correspond to the entity changes
    *
    * Hierarchical listing: First get entity folders under each date folder Select folder matching our entity List that
    * folder for CSV files
    *
    * @return
    *   A stream of rows for this table
    */
  private def getEntityChangeData(
      version: SynapseWatermark,
      batchSchema: ArcaneSchema
  ): ZStream[Any, Throwable, StoredBlob] =
    fetchSortedFiles(version.prefix)

  private def getFileStream(blob: StoredBlob): ZIO[Any, IOException, (BufferedReader, StoredBlob)] =
    reader
      .streamBlobContent(location + blob.name)
      .map(javaReader => (javaReader, blob))
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
      .mapZIO(content => ZIO.attempt(CSVParser.parseCSVLineToRow(content, fileSchema)))
      .mapError(e => new IOException(s"Failed to parse CSV content: ${e.getMessage} from file: $fileName with", e))

  private def isValidSynapseBatch(prefix: String): ZIO[Any, Throwable, Boolean] = hasSchemaFile(prefix)

  /** Get the latest batch folder that is ready for streaming
    * @param previousVersion
    *   Previous valid batch folder
    * @return
    */
  def getCurrentVersion(previousVersion: SynapseWatermark): Task[SynapseWatermark] =
    for synapseBlob <- reader.getCurrentBatch(location).map {
        // in case of a read failure, fallback to previous version - should never happen, but framework expects this method to always succeed
        case version if version.interpretAsDate.isDefined => Some(version)
        case _                                            => None
      }
    yield synapseBlob.map(_.asWatermark).getOrElse(previousVersion)

  /** Check if the provided batch folder has relevant changes - only take a batch that has model.json committed
    * @param latestVersion
    *   Watermark to check for changes
    * @return
    */
  def hasChanges(latestVersion: SynapseWatermark): Task[Boolean] = isValidSynapseBatch(latestVersion.prefix)

  /** Reads changes happened since startFrom date. Inserts and updates are always emitted first, to avoid re-inserting
    * deleted records. Start date to get changes from
    * @return
    */
  def getChanges(version: SynapseWatermark): ZStream[Any, Throwable, StructuredZStream] = reader
    .getEligibleDates(storagePath = location, startFrom = version.timestamp)
    .map(_.asWatermark)
    .mapZIO(wm =>
      getBatchSchema(wm.prefix).flatMap(batchSchema =>
        applySchemaModifications(batchSchema)
          .map(modifiedSchema => (getChangesForVersion(wm, batchSchema), modifiedSchema))
      )
    )

  /** Converts an arbitrary timestamp into a matching watermark
    * @return
    */
  def getWatermark(timestamp: OffsetDateTime): Task[SynapseWatermark] =
    reader.closestDate(location, timestamp).map(_.asWatermark)

  /** Reads changes happened since startFrom date. Inserts and updates are always emitted first, to avoid re-inserting
    * deleted records. Start date to get changes from
    *
    * @return
    */
  private def getChangesForVersion(
      version: SynapseWatermark,
      batchSchema: ArcaneSchema
  ): ZStream[Any, Throwable, DataRow] =
    getEntityChangeData(version, batchSchema)
      .mapZIO(getFileStream)
      .flatMap { case (fileStream, blob) =>
        getTableChanges(fileStream, batchSchema, blob.name)
      }
      .map(convertRow)
      .mapZIO(applyDataRowModifications)

  def getWatermarks(startAt: SynapseWatermark, endAt: SynapseWatermark): Task[Seq[SynapseWatermark]] =
    reader.getDateRange(location, startAt.timestamp, endAt.timestamp).map(_.map(_.asWatermark))

  def getShardFolderStream(folder: String): Task[Option[ShardMetadata]] = for
    watermark <- ZIO.succeed(StoredBlob(name = folder, createdOn = None).asWatermark)
    result <- ZIO.ifZIO(isValidSynapseBatch(watermark.prefix))(
      getBatchSchema(watermark.prefix)
        .flatMap(batchSchema =>
          applySchemaModifications(batchSchema).map(modifiedSchema =>
            Some((stream = (getChangesForVersion(watermark, batchSchema), modifiedSchema), source = watermark.prefix))
          )
        ),
      ZIO.succeed(None)
    )
  yield result

  /** Row type conversions. Should be moved to a separate class, implementing IcebergRowConverter trait, see
    * https://github.com/SneaksAndData/arcane-framework-scala/issues/125
    */

  private def convertRow(row: DataRow): DataRow = applyFieldSelector(row).map(convertCell)

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

  /** Deletes all shards created for the provided streamId
    */
  override def deleteShards(prefix: String): Task[Unit] = ZIO.unit

  /** Retrieve a shard data stream
    *
    * @return
    */
  override def getShards(
      rangeStart: SynapseWatermark,
      rangeEnd: SynapseWatermark
  ): ZStream[Any, Throwable, (stream: (ZStream[Any, Throwable, DataRow], ArcaneSchema), source: String)] =
    ZStream
      .fromZIO(getWatermarks(rangeStart, rangeEnd))
      .flatMap(ZStream.fromIterable(_))
      .filterZIO(wm => isValidSynapseBatch(wm.prefix))
      .mapZIO(wm =>
        getBatchSchema(wm.prefix)
          .map(batchSchema => (stream = (getChangesForVersion(wm, batchSchema), batchSchema), source = wm.prefix))
      )

object SynapseLinkStreamingSource:
  private type SettingsExtractor = PluginStreamContext => MicrosoftSynapseLinkConnectionSettings

  /** ZLayer for SynapseLinkStreamingSource, using custom context extractor.
    * @return
    */
  def getLayer(extractor: SettingsExtractor): ZLayer[PluginStreamContext, Throwable, SynapseLinkStreamingSource] =
    ZLayer {
      for
        context  <- ZIO.service[PluginStreamContext]
        settings <- ZIO.attempt(extractor(context))
      yield new SynapseLinkStreamingSource(
        settings.baseLocation,
        settings.entityName,
        AzureBlobStorageReader(settings.storageConnection),
        context.source.fieldSelectionRule,
        context.source.modifications.modifications
      )
    }
