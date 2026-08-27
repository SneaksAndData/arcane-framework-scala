package com.sneaksanddata.arcane.framework
package services.pullstream

import models.app.PluginStreamContext
import models.schemas.{
  ArcaneSchema,
  ArcaneType,
  DataCell,
  DataRow,
  MergeKeyField,
  MergeableArcaneSchema,
  given_CanAdd_ArcaneSchema
}
import models.settings.TableNaming.parts
import models.settings.sources.pullstream.PullStreamSourceSettings
import logging.ZIOLogAnnotations.zlog
import services.base.{SchemaProvider, StreamingSource}
import services.iceberg.base.SinkPropertyManager
import services.iceberg.given_Conversion_Schema_MergeableArcaneSchema
import services.iceberg.interop.AvroJsonDecoder
import services.pullstream.versioning.PullStreamWatermark
import services.streaming.base.StructuredZStream
import services.iceberg.SchemaConversions.*

import org.apache.avro.Schema as AvroSchema
import org.apache.iceberg.avro.AvroSchemaUtil
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, QueryRequest, QueryResponse, Select}
import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

import java.time.format.DateTimeFormatter
import java.time.{OffsetDateTime, ZoneOffset}
import scala.jdk.CollectionConverters.*

/** This a source that poll output of an Arcane Push Stream Application
  *
  * Arcane push stream lives with the following assumptions: Data is wrapped into the following format:
  *   - primaryKey: primary identifier for the datastream e.g. ProducerId
  *   - secondaryKey: sort-key for the datasteram e.g. TimestampUTC
  *
  * @param primaryKeyFieldName
  *   the field name of the producer
  *
  * @param primaryKeyValue
  *   The value of the producer column
  *
  * @param watermarkFieldName
  *   the field that contains the watermark
  */
/** Sink columns whose values are carried by the DynamoDB item envelope rather than by the `payload` attribute.
  *
  * A column is `None` when the sink does not declare it, in which case the corresponding value is not written at all.
  */
private final case class EnvelopeColumns(watermark: Option[String], mergeKey: Option[String]):
  val names: Set[String] = Set(watermark, mergeKey).flatten

class PullStreamingSource(
    settings: PullStreamSourceSettings,
    dynamodbClient: DynamoDbClient,
    sinkPropertyManager: SinkPropertyManager,
    targetTableFullName: String,
    pageSize: Option[Int]
) extends StreamingSource:

  private val pushPayloadFieldName: String = "payload"
  private val pushIdFieldName: String      = "id"
  private val formatter                    = DateTimeFormatter.ISO_OFFSET_DATE_TIME
  private val listPageSize                 = pageSize.getOrElse(PullStreamingSource.defaultPageSize)

  /** Column used to order concurrent versions of the same merge key during a merge. This is the watermark column, whose
    * value the source appends to every row.
    */
  val versionFieldName: String = settings.watermarkFieldName

  /** Name of the target Iceberg table, without warehouse and namespace. Note that `settings.tableName` refers to the
    * DynamoDB table holding the pushed payloads and must never be used to address the Iceberg sink.
    */
  private val targetTableName: String = targetTableFullName.parts.name

  override def getShards(rangeStart: WatermarkType, rangeEnd: WatermarkType): ZStream[Any, Throwable, ShardMetadata] =
    ZStream.empty

  override def deleteShards(prefix: String): Task[Unit] = ZIO.unit

  override type ShardMetadata = String
  override type WatermarkType = PullStreamWatermark

  override def empty: SchemaType = ArcaneSchema.empty()

  /** Gets the Iceberg schema for the target table.
    *
    * @return
    *   An effect containing the schema.
    */
  override def getSchema: Task[MergeableArcaneSchema] =
    this.sinkPropertyManager.getTableSchema(targetTableName).map(implicitly)

  private def buildQueryGetChanges(latestVersion: PullStreamWatermark): QueryRequest =
    val exprNames = Map(
      "#pk" -> settings.pullIndexKey,
      "#wm" -> settings.watermarkFieldName
    ).asJava

    val exprVals = Map(
      ":pk" -> AttributeValue.builder().s(settings.pullIndexValue).build(),
      ":t"  -> AttributeValue.builder().s(PullStreamingSource.normalizeWatermark(latestVersion.timestamp)).build()
    ).asJava
    QueryRequest
      .builder()
      .tableName(settings.tableName)
      .keyConditionExpression("#pk = :pk AND #wm > :t")
      .expressionAttributeValues(exprVals)
      .expressionAttributeNames(exprNames)
      .limit(listPageSize)
      .build()

  private def buildQueryHasChanges(latestVersion: PullStreamWatermark): QueryRequest =
    val exprNames = Map(
      "#pk" -> settings.pullIndexKey,
      "#wm" -> settings.watermarkFieldName
    ).asJava

    val exprVals = Map(
      ":pk" -> AttributeValue.builder().s(settings.pullIndexValue).build(),
      ":t"  -> AttributeValue.builder().s(PullStreamingSource.normalizeWatermark(latestVersion.timestamp)).build()
    ).asJava
    QueryRequest
      .builder()
      .tableName(settings.tableName)
      .keyConditionExpression("#pk = :pk AND #wm > :t")
      .expressionAttributeValues(exprVals)
      .expressionAttributeNames(exprNames)
      .limit(1)
      .select(Select.COUNT)
      .build()

  private def buildQueryMaxTimestamp: QueryRequest =
    val exprNames = Map(
      "#pk" -> settings.pullIndexKey,
      "#wm" -> settings.watermarkFieldName
    ).asJava

    val exprVals = Map(
      ":pk" -> AttributeValue.builder().s(settings.pullIndexValue).build()
    ).asJava
    QueryRequest
      .builder()
      .tableName(settings.tableName)
      .keyConditionExpression("#pk = :pk")
      .expressionAttributeValues(exprVals)
      .expressionAttributeNames(exprNames)
      .scanIndexForward(false) // descending on range key -> first item should be highest wm
      .limit(1)
      .projectionExpression("#wm")
      .build()

  private def runDynamoQuery(queryRequest: QueryRequest): Task[QueryResponse] =
    for
      response <- ZIO.attemptBlocking(dynamodbClient.query(queryRequest))
      hasMore   = Option(response.lastEvaluatedKey()).exists(!_.isEmpty)
      itemCount = Option(response.items()).map(_.size()).getOrElse(0)
      _ <-
        if hasMore then
          zlog(
            "DynamoDB query on table '%s' returned a truncated response (%s items in this page, additional pages available but not fetched by this call)",
            settings.tableName,
            itemCount.toString
          )
        else
          zlog(
            "DynamoDB query on table '%s' returned a complete response (%s items, no further pages)",
            settings.tableName,
            itemCount.toString
          )
    yield response

  /** Executes the given query and transparently follows `LastEvaluatedKey`, returning one `QueryResponse` per page. The
    * stream terminates when DynamoDB returns no continuation key. Emits an info log for each page indicating the page
    * index, item count and whether more pages will follow.
    */
  private def paginatedQuery(request: QueryRequest): ZStream[Any, Throwable, QueryResponse] =
    // State: (pageIndex, itemsSoFar, Option[startKey]). pageIndex starts at 1 for the first response.
    ZStream.paginateZIO((1, 0L, Option.empty[Map[String, AttributeValue]])) { case (pageIndex, itemsSoFar, startKey) =>
      val pagedRequest = startKey.fold(request)(k => request.toBuilder.exclusiveStartKey(k.asJava).build())
      for
        response <- ZIO.attemptBlocking(dynamodbClient.query(pagedRequest))
        pageItemCount = Option(response.items()).map(_.size()).getOrElse(0)
        totalItems    = itemsSoFar + pageItemCount
        nextKey       = Option(response.lastEvaluatedKey()).map(_.asScala.toMap).filter(_.nonEmpty)
        hasMore       = nextKey.isDefined
        _ <-
          if pageIndex == 1 && !hasMore then
            zlog(
              "DynamoDB paginated query on table '%s' completed in a single page (%s items, no pagination needed)",
              settings.tableName,
              pageItemCount.toString
            )
          else
            zlog(
              "DynamoDB paginated query on table '%s' page %s returned %s items (total so far: %s, more pages: %s)",
              settings.tableName,
              pageIndex.toString,
              pageItemCount.toString,
              totalItems.toString,
              hasMore.toString
            )
        next = nextKey.map(k => (pageIndex + 1, totalItems, Some(k)))
      yield response -> next
    }

  private def getSchemaInfo: Task[
    (avro: AvroSchema, iceberg: org.apache.iceberg.Schema, envelope: EnvelopeColumns, pointer: Option[String])
  ] =
    for
      icebergSchema <- this.sinkPropertyManager.getTableSchema(targetTableName)
      pointer       <- resolveJsonPointer
    yield {
      val envelope = EnvelopeColumns(
        watermark = resolveColumn(icebergSchema, settings.watermarkFieldName),
        mergeKey = resolveColumn(icebergSchema, MergeKeyField.name)
      )
      // Envelope columns are carried by the DynamoDB item, not by `payload`, so they are hidden from the decoder:
      // under strict decoding a column that the payload never carries would fail the whole batch. They are appended
      // to each row after decoding instead. The unpruned schema is still returned, since the staging table must
      // declare the columns for their values to be written.
      val payloadColumns = icebergSchema
        .columns()
        .asScala
        .filterNot(column => envelope.names.contains(column.name()))
        .asJava

      (
        AvroSchemaUtil.convert(org.apache.iceberg.Schema(payloadColumns), targetTableName),
        icebergSchema,
        envelope,
        pointer
      )
    }

  /** Resolves the JSON pointer applied to each stored document before decoding.
    *
    * The producing side publishes it on the sink table, because it is the same pointer the table's columns were derived
    * from and the stream's own configuration has no field to carry it. Reading it back from the table is what keeps a
    * route's columns and the document they describe in step, without the two services having to agree on anything but
    * the table itself. A pointer given in the settings still wins, so an operator can override a table that carries a
    * stale value, and a table that carries none behaves as it always did: decoding from the root.
    */
  private def resolveJsonPointer: Task[Option[String]] =
    settings.jsonPointerExpression.filter(_.nonEmpty) match
      case configured @ Some(_) => ZIO.succeed(configured)
      case None =>
        this.sinkPropertyManager
          .getProperty(targetTableName, PullStreamingSource.jsonPointerPropertyName)
          .map(_.filter(_.nonEmpty))

  /** Locates the sink column that receives an envelope attribute.
    *
    * The lookup is case-insensitive but returns the column's own spelling, because rows are matched to Iceberg fields
    * by exact name and engines that fold unquoted identifiers create the column lowercased regardless of how the
    * attribute is spelled. `None` means the sink does not store the attribute, in which case it is left out of the rows
    * entirely.
    */
  private def resolveColumn(icebergSchema: org.apache.iceberg.Schema, attributeName: String): Option[String] =
    icebergSchema.columns().asScala.map(_.name()).find(_.equalsIgnoreCase(attributeName))

  /** Parse the dynamodb query response into DataRows.
    *
    * Only `payload` holds producer data; the remaining item attributes are envelope metadata. The watermark and the
    * merge key are appended to every decoded row whenever the sink declares a column for them, so the ingestion
    * timestamp and the row identity are persisted alongside the payload instead of being discarded.
    *
    * The item iterable is chunked explicitly rather than at ZIO's default of 4096: a chunk is decoded as a unit, so
    * letting a whole page land in one chunk would hold every decoded row of that page in memory at once. A page is up
    * to `listPageSize` items of raw JSON, and decoding amplifies that into DataRows, which is where the peak sits.
    */
  private def responseStream(
      queryResponse: QueryResponse,
      avroSchema: AvroSchema,
      envelope: EnvelopeColumns,
      jsonPointer: Option[String]
  ): ZStream[Any, Throwable, DataRow] =
    // nested documents land in Types.VariantType columns on the sink, whose parquet writer expects a Variant
    val decoder = new AvroJsonDecoder(
      schema = avroSchema,
      jsonPointerExpr = jsonPointer,
      tolerateMissingFields = false
    )

    ZStream
      .fromIterable(queryResponse.items().asScala, PullStreamingSource.decodeChunkSize)
      .map { item =>
        val attributes = item.asScala
        def stringAttribute(name: String): Option[String] =
          attributes.get(name).flatMap(attribute => Option(attribute.s()))

        // TOOD: in 2.4 version this becomes dataRowModification SurrogateVersion
        val watermarkCell =
          for
            column         <- envelope.watermark
            watermarkValue <- stringAttribute(settings.watermarkFieldName)
          yield DataCell(column, ArcaneType.StringType, watermarkValue)

        // the merge key identifies the target row: the payload carries no identity of its own, so the id attribute
        // of the DynamoDB item is used, giving one target row per pushed message.
        // Unlike the watermark, the cell is named after MergeKeyField rather than after the sink column: the schema
        // conversion re-tags any case-insensitive match as an IndexedMergeKeyField, which reports the canonical
        // upper-case name, and it appends that field outright when the sink declares no column at all. The staging
        // table therefore always carries the canonical spelling, and a cell named after the sink would not match it.

        // TOOD: in 2.4 version this becomes dataRowModification SurrogateMergeKey
        val mergeKeyCell =
          stringAttribute(pushIdFieldName)
            .map(mergeKeyValue => DataCell(MergeKeyField.name, MergeKeyField.fieldType, mergeKeyValue))

        (attributes(pushPayloadFieldName).s(), watermarkCell ++ mergeKeyCell)
      }
      .mapZIO { case (payload, envelopeCells) =>
        ZIO.attempt(decoder.parse(payload).map(row => row ++ envelopeCells))
      }
      .flatMap(rows => ZStream.fromIterable(rows))

  /** Gets the changes in the database since the given version.
    *
    * @param previousVersion
    *   The version to fetch changes from.
    * @return
    *   An effect containing the changes in the database since the given version and the latest observed version.
    */
  def getChanges(previousVersion: PullStreamWatermark): ZStream[Any, Throwable, StructuredZStream] =
    ZStream.fromZIO(getSchemaInfo).map { case (avroSchema, icebergSchema, envelope, jsonPointer) =>
      val rowStream: ZStream[Any, Throwable, DataRow] =
        paginatedQuery(buildQueryGetChanges(previousVersion))
          .flatMap(response => responseStream(response, avroSchema, envelope, jsonPointer))
      (rowStream, icebergSchema)
    }

  /** Returns true if the queue has new rows.
    *
    * @param previousVersion
    *   The latest watermark that was already checked.
    * @return
    *   true if new rows are present
    */
  def hasRows(previousVersion: PullStreamWatermark): Task[Boolean] =
    runDynamoQuery(buildQueryHasChanges(previousVersion))
      .map(_.count() > 0)

  def getMaxTimestamp: Task[PullStreamWatermark] = runDynamoQuery(buildQueryMaxTimestamp)
    .map(
      _.items().asScala.headOption
        .map(_.asScala.head._2.s())
        .map(timeString => PullStreamWatermark(OffsetDateTime.parse(timeString, formatter)))
    )
    .map(_.getOrElse(PullStreamWatermark.epoch))

object PullStreamingSource:

  /** Sink table property carrying the JSON pointer that selects the part of each stored document holding the data.
    *
    * Written by the producing service when it provisions the table, from the same setting its columns were derived
    * from. Absent on a table whose documents are decoded from their root.
    */
  val jsonPointerPropertyName: String = "json-pointer-expression"

  /** Default page size passed as `Limit` to each DynamoDB `Query` request. DynamoDB caps the response payload at 1 MB
    * per page anyway, so this is a soft upper bound on items evaluated per network call, not a total-result cap.
    */
  val defaultPageSize: Int = 1000

  /** Items decoded as a single unit while a page is turned into rows.
    *
    * ZIO's default is 4096, which for this stream would place an entire page in one chunk: `mapZIO` materializes a
    * whole chunk before emitting it, so every decoded row of the page would be live at once. Decoding amplifies raw
    * JSON several times over, so the bound is kept well below a page.
    */
  val decodeChunkSize: Int = 32

  /** Normalizes a watermark timestamp to a lexicographically comparable ISO-8601 string in UTC. The DynamoDB sort key
    * is a string, so mixed offsets (`+02:00` vs `+00:00`) would order incorrectly under `wm > :t`. Producers are
    * expected to write UTC (`Z`) values; this ensures the *reader* side does the same when constructing the key
    * condition.
    */
  def normalizeWatermark(timestamp: OffsetDateTime): String =
    timestamp.withOffsetSameInstant(ZoneOffset.UTC).toString

  type Environment               = PluginStreamContext & DynamoDbClient & SinkPropertyManager
  private type SettingsExtractor = PluginStreamContext => PullStreamSourceSettings

  def getLayer(
      extractor: SettingsExtractor
  ): ZLayer[Environment, Nothing, PullStreamingSource & SchemaProvider[ArcaneSchema]] =
    ZLayer {
      for
        context         <- ZIO.service[PluginStreamContext]
        settings        <- ZIO.succeed(extractor(context))
        dynamodbClient  <- ZIO.service[DynamoDbClient]
        propertyManager <- ZIO.service[SinkPropertyManager]
      yield PullStreamingSource(
        settings,
        dynamodbClient,
        propertyManager,
        context.sink.targetTableFullName,
        settings.pageSize
      )
    }
