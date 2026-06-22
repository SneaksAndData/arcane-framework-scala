package com.sneaksanddata.arcane.framework
package services.pushstream

import services.base.StreamingSource

import com.fasterxml.jackson.databind.node.ObjectNode
import com.sneaksanddata.arcane.framework.models.schemas.{ArcaneSchema, DataRow, given_CanAdd_ArcaneSchema}
import com.sneaksanddata.arcane.framework.models.settings.TableNaming.parts
import com.sneaksanddata.arcane.framework.services.iceberg.base.SinkPropertyManager
import com.sneaksanddata.arcane.framework.services.pushstream.versioning.PushStreamWatermark
import com.sneaksanddata.arcane.framework.services.streaming.base.StructuredZStream
import com.sneaksanddata.arcane.framework.services.iceberg.given_Conversion_Schema_ArcaneSchema
import com.sneaksanddata.arcane.framework.services.iceberg.given_Conversion_AvroGenericRecord_DataRow

import org.apache.avro.Schema as AvroSchema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.iceberg.avro.AvroSchemaUtil
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, QueryRequest, QueryResponse, Select}
import zio.{Task, ZIO}
import zio.stream.ZStream

import scala.jdk.CollectionConverters.*

/** This a source that poll output of an Arcane Push Stream Application
  */
class PushStreamingSource(
    sourceTableName: String,
    targetTableName: String,
    dynamodbClient: DynamoDbClient,
    sinkPropertyManager: SinkPropertyManager
) extends StreamingSource:

  private val pushPayloadFieldName: String = "payload"

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
    // TODO: pushPayloadFieldName
    val tableName    = "test"
    val partitionKey = "prducer/number1"
    val exprVals = Map(
      ":pk"            -> AttributeValue.builder().s(partitionKey).build(),
      ":latestVersion" -> AttributeValue.builder().s(latestVersion.toString).build()
    ).asJava
    QueryRequest
      .builder()
      .tableName(tableName)
      .keyConditionExpression("pk = :pk AND timestampUTC > :t")
      .expressionAttributeValues(exprVals)
      .limit(1)
      .select(Select.COUNT)
      .build()

  private def runDynamoQuery(queryRequest: QueryRequest): Task[QueryResponse] =
    for result <- ZIO.attemptBlocking(dynamodbClient.query(queryRequest))
    yield result

  private def getAvroSchema: Task[AvroSchema] = this.sinkPropertyManager
    .getTableSchema(sourceTableName)
    .map(icebergSchema => AvroSchemaUtil.convert(icebergSchema, targetTableName.parts.name))

  // TODO: change JSON scanner so it can accept plain string input instead of a filepath

  // TODO: remove this code
  private def getAvroReader(schema: AvroSchema) = GenericDatumReader[GenericRecord](schema)
  private val jsonMapper                        = com.fasterxml.jackson.databind.ObjectMapper()

  private def decodeJson(node: ObjectNode, schema: AvroSchema, reader: GenericDatumReader[GenericRecord]): DataRow =
    val decoder = DecoderFactory.get().jsonDecoder(schema, node.toString)
    reader.read(null, decoder)

  private def responseStream(queryResponse: QueryResponse): ZStream[Any, Throwable, DataRow] = ZStream
    .fromZIO(getAvroSchema)
    .flatMap { avroSchema =>
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
    }

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
      getSchema.map(schema => (responseStream(response), schema))
    }
