package com.sneaksanddata.arcane.framework
package services.pushstream

import models.app.PluginStreamContext
import models.schemas.{ArcaneSchema, DataRow, given_CanAdd_ArcaneSchema}
import models.settings.TableNaming.parts
import models.settings.sources.pushstream.PushStreamSourceSettings
import services.base.{SchemaProvider, StreamingSource}
import services.iceberg.base.SinkPropertyManager
import services.iceberg.given_Conversion_Schema_ArcaneSchema
import services.iceberg.interop.AvroJsonDecoder
import services.pushstream.versioning.PushStreamWatermark
import services.streaming.base.StructuredZStream

import org.apache.avro.Schema as AvroSchema
import org.apache.iceberg.avro.AvroSchemaUtil
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, QueryRequest, QueryResponse, Select}
import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
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
class PushStreamingSource(
    settings: PushStreamSourceSettings,
    dynamodbClient: DynamoDbClient,
    sinkPropertyManager: SinkPropertyManager
) extends StreamingSource:

  import settings.{
    primaryKeyFieldName,
    primaryKeyValue,
    sourceTableName,
    targetTableName,
    watermarkFieldName
  }

  private val pushPayloadFieldName: String = "payload"
  private val formatter                    = DateTimeFormatter.ISO_OFFSET_DATE_TIME

  override def getShards(rangeStart: WatermarkType, rangeEnd: WatermarkType): ZStream[Any, Throwable, ShardMetadata] =
    ZStream.empty

  override def deleteShards(prefix: String): Task[Unit] = ZIO.unit

  override type ShardMetadata = String
  override type WatermarkType = PushStreamWatermark

  override def empty: SchemaType = ArcaneSchema.empty()

  /** Gets the Iceberg schema for the table in the database.
    *
    * @return
    *   An effect containing the schema.
    */
  override def getSchema: Task[ArcaneSchema] =
    this.sinkPropertyManager.getTableSchema(sourceTableName).map(implicitly)

  private def buildQueryGetChanges(latestVersion: PushStreamWatermark, limit: Int = 100): QueryRequest =
    val exprNames = Map(
      "#pk" -> primaryKeyFieldName,
      "#wm" -> watermarkFieldName
    ).asJava

    val exprVals = Map(
      ":pk" -> AttributeValue.builder().s(primaryKeyValue).build(),
      ":t"  -> AttributeValue.builder().s(latestVersion.timestamp.toString).build()
    ).asJava
    QueryRequest
      .builder()
      .tableName(sourceTableName)
      .keyConditionExpression("#pk = :pk AND #wm > :t")
      .expressionAttributeValues(exprVals)
      .expressionAttributeNames(exprNames)
      .limit(limit)
      .build()

  private def buildQueryHasChanges(latestVersion: PushStreamWatermark): QueryRequest =
    val exprNames = Map(
      "#pk" -> primaryKeyFieldName,
      "#wm" -> watermarkFieldName
    ).asJava

    val exprVals = Map(
      ":pk" -> AttributeValue.builder().s(primaryKeyValue).build(),
      ":t"  -> AttributeValue.builder().s(latestVersion.timestamp.toString).build()
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
    ZIO.attemptBlocking(dynamodbClient.query(queryRequest))

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
  def getChanges(previousVersion: PushStreamWatermark): ZStream[Any, Throwable, StructuredZStream] = ZStream
    .fromZIO(runDynamoQuery(buildQueryGetChanges(previousVersion)))
    .mapZIO { response =>
      // TODO: add paginated response (and chunking)
      getSchemaInfo.map { case (avroSchema, icebergSchema) =>
        (responseStream(response, avroSchema), icebergSchema)
      }
    }

  /** Returns true if the queue has new rows.
    *
    * @param previousVersion
    *   The latest watermark that was already checked.
    * @return
    *   true if new rows are present
    */
  def hasRows(previousVersion: PushStreamWatermark): Task[Boolean] =
    runDynamoQuery(buildQueryHasChanges(previousVersion))
      .map(_.count() > 0)

  def getMaxTimestamp: Task[PushStreamWatermark] = runDynamoQuery(buildQueryMaxTimestamp)
    .map(
      _.items().asScala.headOption
        .map(_.asScala.head._2.s())
        .map(timeString => PushStreamWatermark(OffsetDateTime.parse(timeString, formatter)))
    )
    .map(_.getOrElse(PushStreamWatermark.epoch))

object PushStreamingSource:

  type Environment               = PluginStreamContext & DynamoDbClient & SinkPropertyManager
  private type SettingsExtractor = PluginStreamContext => PushStreamSourceSettings

  def apply(
      settings: PushStreamSourceSettings,
      dynamodbClient: DynamoDbClient,
      sinkPropertyManager: SinkPropertyManager
  ): PushStreamingSource =
    new PushStreamingSource(settings, dynamodbClient, sinkPropertyManager)

  def getLayer(
      extractor: SettingsExtractor
  ): ZLayer[Environment, Nothing, PushStreamingSource & SchemaProvider[ArcaneSchema]] =
    ZLayer {
      for
        context         <- ZIO.service[PluginStreamContext]
        dynamodbClient  <- ZIO.service[DynamoDbClient]
        propertyManager <- ZIO.service[SinkPropertyManager]
      yield PushStreamingSource(extractor(context), dynamodbClient, propertyManager)
    }

