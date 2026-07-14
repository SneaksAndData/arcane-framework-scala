package com.sneaksanddata.arcane.framework
package services.pullstream

import models.app.PluginStreamContext
import models.schemas.{ArcaneSchema, DataRow, MergeableArcaneSchema, given_CanAdd_ArcaneSchema}
import models.settings.TableNaming.parts
import models.settings.sources.pullstream.PullStreamSourceSettings
import logging.ZIOLogAnnotations.zlog
import services.base.{SchemaProvider, StreamingSource}
import services.iceberg.base.SinkPropertyManager
import services.iceberg.given_Conversion_Schema_MergeableArcaneSchema
import services.iceberg.interop.AvroJsonDecoder
import services.pullstream.versioning.PullStreamWatermark
import services.streaming.base.StructuredZStream

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
class PullStreamingSource(
    settings: PullStreamSourceSettings,
    dynamodbClient: DynamoDbClient,
    sinkPropertyManager: SinkPropertyManager,
    pageSize: Int = PullStreamingSource.defaultPageSize
) extends StreamingSource:

  import settings.{primaryKeyFieldName, primaryKeyValue, sourceTableName, targetTableName, watermarkFieldName}

  private val pushPayloadFieldName: String = "payload"
  private val formatter                    = DateTimeFormatter.ISO_OFFSET_DATE_TIME

  override def getShards(rangeStart: WatermarkType, rangeEnd: WatermarkType): ZStream[Any, Throwable, ShardMetadata] =
    ZStream.empty

  override def deleteShards(prefix: String): Task[Unit] = ZIO.unit

  override type ShardMetadata = String
  override type WatermarkType = PullStreamWatermark

  override def empty: SchemaType = ArcaneSchema.empty()

  /** Gets the Iceberg schema for the table in the database.
    *
    * @return
    *   An effect containing the schema.
    */
  override def getSchema: Task[ArcaneSchema] =
    this.sinkPropertyManager.getTableSchema(sourceTableName).map(s => (s: MergeableArcaneSchema))

  private def buildQueryGetChanges(latestVersion: PullStreamWatermark): QueryRequest =
    val exprNames = Map(
      "#pk" -> primaryKeyFieldName,
      "#wm" -> watermarkFieldName
    ).asJava

    val exprVals = Map(
      ":pk" -> AttributeValue.builder().s(primaryKeyValue).build(),
      ":t"  -> AttributeValue.builder().s(PullStreamingSource.normalizeWatermark(latestVersion.timestamp)).build()
    ).asJava
    QueryRequest
      .builder()
      .tableName(sourceTableName)
      .keyConditionExpression("#pk = :pk AND #wm > :t")
      .expressionAttributeValues(exprVals)
      .expressionAttributeNames(exprNames)
      .limit(pageSize)
      .build()

  private def buildQueryHasChanges(latestVersion: PullStreamWatermark): QueryRequest =
    val exprNames = Map(
      "#pk" -> primaryKeyFieldName,
      "#wm" -> watermarkFieldName
    ).asJava

    val exprVals = Map(
      ":pk" -> AttributeValue.builder().s(primaryKeyValue).build(),
      ":t"  -> AttributeValue.builder().s(PullStreamingSource.normalizeWatermark(latestVersion.timestamp)).build()
    ).asJava
    QueryRequest
      .builder()
      .tableName(sourceTableName)
      .keyConditionExpression("#pk = :pk AND #wm > :t")
      .expressionAttributeValues(exprVals)
      .expressionAttributeNames(exprNames)
      .limit(1)
      .select(Select.COUNT)
      .build()

  private def buildQueryMaxTimestamp: QueryRequest =
    val exprNames = Map(
      "#pk" -> primaryKeyFieldName,
      "#wm" -> watermarkFieldName
    ).asJava

    val exprVals = Map(
      ":pk" -> AttributeValue.builder().s(primaryKeyValue).build()
    ).asJava
    QueryRequest
      .builder()
      .tableName(sourceTableName)
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
      hasMore = Option(response.lastEvaluatedKey()).exists(k => k != null && !k.isEmpty)
      itemCount = Option(response.items()).map(_.size()).getOrElse(0)
      _ <-
        if hasMore then
          zlog(
            "DynamoDB query on table '%s' returned a truncated response (%s items in this page, additional pages available but not fetched by this call)",
            sourceTableName,
            itemCount.toString
          )
        else
          zlog(
            "DynamoDB query on table '%s' returned a complete response (%s items, no further pages)",
            sourceTableName,
            itemCount.toString
          )
    yield response

  /** Executes the given query and transparently follows `LastEvaluatedKey`, returning one
    * `QueryResponse` per page. The stream terminates when DynamoDB returns no continuation key.
    * Emits an info log for each page indicating the page index, item count and whether more
    * pages will follow.
    */
  private def paginatedQuery(request: QueryRequest): ZStream[Any, Throwable, QueryResponse] =
    // State: (pageIndex, itemsSoFar, Option[startKey]). pageIndex starts at 1 for the first response.
    ZStream.paginateZIO((1, 0L, Option.empty[java.util.Map[String, AttributeValue]])) {
      case (pageIndex, itemsSoFar, startKey) =>
        val pagedRequest = startKey.fold(request)(k => request.toBuilder.exclusiveStartKey(k).build())
        for
          response <- ZIO.attemptBlocking(dynamodbClient.query(pagedRequest))
          pageItemCount = Option(response.items()).map(_.size()).getOrElse(0)
          totalItems    = itemsSoFar + pageItemCount
          nextKey = Option(response.lastEvaluatedKey())
            .filter(k => k != null && !k.isEmpty)
          hasMore = nextKey.isDefined
          _ <-
            if pageIndex == 1 && !hasMore then
              zlog(
                "DynamoDB paginated query on table '%s' completed in a single page (%s items, no pagination needed)",
                sourceTableName,
                pageItemCount.toString
              )
            else
              zlog(
                "DynamoDB paginated query on table '%s' page %s returned %s items (total so far: %s, more pages: %s)",
                sourceTableName,
                pageIndex.toString,
                pageItemCount.toString,
                totalItems.toString,
                hasMore.toString
              )
          next = nextKey.map(k => (pageIndex + 1, totalItems, Some(k)))
        yield response -> next
    }

  private def getSchemaInfo: Task[(avro: AvroSchema, iceberg: org.apache.iceberg.Schema)] =
    this.sinkPropertyManager
      .getTableSchema(sourceTableName)
      .map(icebergSchema => (AvroSchemaUtil.convert(icebergSchema, targetTableName.parts.name), icebergSchema))

  /** Parse the dynamodb query response into DataRows
    */
  private def responseStream(queryResponse: QueryResponse, avroSchema: AvroSchema): ZStream[Any, Throwable, DataRow] =
    val decoder = AvroJsonDecoder(avroSchema, tolerateMissingFields = false)

    ZStream
      .fromIterable(queryResponse.items().asScala)
      .map(
        _.asScala
          .collect {
            case (fieldName, fieldValue) if fieldName == pushPayloadFieldName => fieldValue
          }
          .head
          .s()
      )
      .mapZIO(line => ZIO.attempt(decoder.parse(line)))
      .flatMap(rows => ZStream.fromIterable(rows))

  /** Gets the changes in the database since the given version.
    *
    * @param previousVersion
    *   The version to fetch changes from.
    * @return
    *   An effect containing the changes in the database since the given version and the latest observed version.
    */
  def getChanges(previousVersion: PullStreamWatermark): ZStream[Any, Throwable, StructuredZStream] =
    ZStream.fromZIO(getSchemaInfo).map { case (avroSchema, icebergSchema) =>
      val rowStream: ZStream[Any, Throwable, DataRow] =
        paginatedQuery(buildQueryGetChanges(previousVersion))
          .flatMap(response => responseStream(response, avroSchema))
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

  /** Default page size passed as `Limit` to each DynamoDB `Query` request. DynamoDB caps the
    * response payload at 1 MB per page anyway, so this is a soft upper bound on items evaluated
    * per network call, not a total-result cap.
    */
  val defaultPageSize: Int = 1000

  /** Normalizes a watermark timestamp to a lexicographically comparable ISO-8601 string in UTC.
    * The DynamoDB sort key is a string, so mixed offsets (`+02:00` vs `+00:00`) would order
    * incorrectly under `wm > :t`. Producers are expected to write UTC (`Z`) values; this ensures
    * the *reader* side does the same when constructing the key condition.
    */
  def normalizeWatermark(timestamp: OffsetDateTime): String =
    timestamp.withOffsetSameInstant(ZoneOffset.UTC).toString

  type Environment               = PluginStreamContext & DynamoDbClient & SinkPropertyManager
  private type SettingsExtractor = PluginStreamContext => PullStreamSourceSettings

  def apply(
      settings: PullStreamSourceSettings,
      dynamodbClient: DynamoDbClient,
      sinkPropertyManager: SinkPropertyManager
  ): PullStreamingSource =
    new PullStreamingSource(settings, dynamodbClient, sinkPropertyManager)

  def apply(
      settings: PullStreamSourceSettings,
      dynamodbClient: DynamoDbClient,
      sinkPropertyManager: SinkPropertyManager,
      pageSize: Int
  ): PullStreamingSource =
    new PullStreamingSource(settings, dynamodbClient, sinkPropertyManager, pageSize)

  def getLayer(
      extractor: SettingsExtractor
  ): ZLayer[Environment, Nothing, PullStreamingSource & SchemaProvider[ArcaneSchema]] =
    ZLayer {
      for
        context         <- ZIO.service[PluginStreamContext]
        dynamodbClient  <- ZIO.service[DynamoDbClient]
        propertyManager <- ZIO.service[SinkPropertyManager]
      yield PullStreamingSource(extractor(context), dynamodbClient, propertyManager)
    }
