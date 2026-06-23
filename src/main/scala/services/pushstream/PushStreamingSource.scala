package com.sneaksanddata.arcane.framework
package services.pushstream

import models.schemas.{ArcaneSchema, DataRow, given_CanAdd_ArcaneSchema}
import models.settings.TableNaming.parts
import services.base.StreamingSource
import services.iceberg.base.SinkPropertyManager
import services.iceberg.{given_Conversion_AvroGenericRecord_DataRow, given_Conversion_Schema_ArcaneSchema}
import services.pushstream.versioning.PushStreamWatermark
import services.streaming.base.StructuredZStream

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.avro.Schema as AvroSchema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.iceberg.avro.AvroSchemaUtil
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, QueryRequest, QueryResponse, Select}
import zio.stream.ZStream
import zio.{Task, ZIO}
import zio.Console

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import scala.jdk.CollectionConverters.*

/** This a source that poll output of an Arcane Push Stream Application
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
    sourceTableName: String,
    targetTableName: String,
    primaryKeyFieldName: String,
    primaryKeyValue: String,
    watermarkFieldName: String,
    dynamodbClient: DynamoDbClient,
    sinkPropertyManager: SinkPropertyManager
) extends StreamingSource:

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

  private def buildQueryGetChanges(latestVersion: WatermarkType): QueryRequest =
    val tableName    = "test"
    val partitionKey = "prducer/number1"
    val exprVals = Map(
      ":pk"            -> AttributeValue.builder().s(partitionKey).build(),
      ":latestVersion" -> AttributeValue.builder().s(latestVersion.toString).build()
    ).asJava
    // TODO: build
    QueryRequest.builder().build()

  private def buildQueryHasChanges(latestVersion: PushStreamWatermark): QueryRequest =
    val exprNames = Map(
      "#pk" -> primaryKeyFieldName,
      "#wm" -> watermarkFieldName
    ).asJava

    val exprVals = Map(
      ":pk" -> AttributeValue.builder().s(primaryKeyValue).build(),
      ":t"  -> AttributeValue.builder().s(latestVersion.toString).build()
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

  private def buildQueryMaxTimestamp: QueryRequest = ???

  private def runDynamoQuery(queryRequest: QueryRequest): Task[QueryResponse] =
    for
      result <- ZIO.attemptBlocking(dynamodbClient.query(queryRequest))
      _      <- Console.printLine(result)
    yield result

  private def getSchemaInfo: Task[(avro: AvroSchema, iceberg: org.apache.iceberg.Schema)] = this.sinkPropertyManager
    .getTableSchema(sourceTableName)
    .map(icebergSchema => (AvroSchemaUtil.convert(icebergSchema, targetTableName.parts.name), icebergSchema))

  // TODO: change JSON scanner so it can accept plain string input instead of a filepath

  // TODO: remove this code
  private def getAvroReader(schema: AvroSchema) = GenericDatumReader[GenericRecord](schema)
  private val jsonMapper                        = com.fasterxml.jackson.databind.ObjectMapper()

  private def decodeJson(node: ObjectNode, schema: AvroSchema, reader: GenericDatumReader[GenericRecord]): DataRow =
    val decoder = DecoderFactory.get().jsonDecoder(schema, node.toString)
    reader.read(null, decoder)

  private def responseStream(queryResponse: QueryResponse, avroSchema: AvroSchema): ZStream[Any, Throwable, DataRow] =
    val avroReader = getAvroReader(avroSchema)

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
      .map(jsonMapper.readTree)
      .map(node => decodeJson(node.asInstanceOf[ObjectNode], avroSchema, avroReader))

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
      getSchemaInfo.map { case (avroSchema, icebergSchema) =>
        (responseStream(response, avroSchema), icebergSchema)
      }
    }

  def hasRows(previousVersion: PushStreamWatermark): Task[Boolean] = for {
    _ <- Console.printLine("hasRows")
    res <- runDynamoQuery(buildQueryHasChanges(previousVersion))
      .map(_.hasItems())
  } yield res

  def getMaxTimestamp: Task[PushStreamWatermark] = runDynamoQuery(buildQueryMaxTimestamp)
    .map(
      _.items().asScala.headOption
        .map(_.asScala.head._2.s())
        .map(timeString => PushStreamWatermark(OffsetDateTime.parse(timeString, formatter)))
    )
    .map(_.getOrElse(PushStreamWatermark.epoch))
