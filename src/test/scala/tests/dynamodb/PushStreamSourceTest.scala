package com.sneaksanddata.arcane.framework
package tests.dynamodb

import models.ddl.CreateTableRequest as IcebergCreateTableRequest
import models.schemas.{ArcaneSchema, ArcaneType, Field}
import services.iceberg.SchemaConversions.toIcebergSchemaFromFields
import services.pushstream.PushStreamingSource
import services.pushstream.versioning.PushStreamWatermark
import tests.shared.{IcebergUtil, TestDynamicSinkSettings}

import software.amazon.awssdk.auth.credentials.*
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, Task, ZIO}

import java.net.URI
import java.time.format.DateTimeFormatter
import java.time.{Instant, OffsetDateTime, ZoneOffset}
import scala.jdk.CollectionConverters.*
import scala.language.postfixOps
// TODO: add SourceDataProvider tests
object PushStreamTestServices:
  val access_kid    = "test"
  val access_secret = "test"

  def getClient: Task[DynamoDbClient] =
    ZIO.attempt(
      DynamoDbClient
        .builder()
        .endpointOverride(URI.create("http://localhost:8000"))
        .region(Region.US_EAST_1)
        .credentialsProvider(
          StaticCredentialsProvider.create(AwsBasicCredentials.create(access_kid, access_secret))
        )
        .build()
    )

  def createTable(tableName: String, client: DynamoDbClient): Task[CreateTableResponse] = for {
    req = CreateTableRequest
      .builder()
      .tableName(tableName)
      .keySchema(
        KeySchemaElement.builder().attributeName("producer").keyType(KeyType.HASH).build(),
        KeySchemaElement.builder().attributeName("timestampUTC").keyType(KeyType.RANGE).build()
      )
      .attributeDefinitions(
        AttributeDefinition.builder().attributeName("producer").attributeType(ScalarAttributeType.S).build(),
        AttributeDefinition.builder().attributeName("timestampUTC").attributeType(ScalarAttributeType.S).build()
      )
      .provisionedThroughput(
        ProvisionedThroughput
          .builder()
          .readCapacityUnits(5L)
          .writeCapacityUnits(5L)
          .build()
      )
      .build()
    r <- ZIO.attemptBlocking(client.createTable(req))
  } yield r

  def listTables(client: DynamoDbClient): Task[List[String]] = ZIO
    .attemptBlocking(
      client.listTables(ListTablesRequest.builder().build())
    )
    .map(_.tableNames().toArray.toList.map(_.toString))

  def deleteTable(client: DynamoDbClient, tableName: String): Task[DeleteTableResponse] =
    ZIO.attemptBlocking(client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build()))

  def insertData(
      client: DynamoDbClient,
      tableName: String,
      primaryKeyField: String,
      primaryKeyValue: String,
      watermarkField: String
  ): Task[PutItemResponse] = for {
    watermarkValue = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC).plusHours(1)
    item = Map(
      primaryKeyField -> AttributeValue.builder().s(primaryKeyValue).build(),
      watermarkField  -> AttributeValue.builder().s(watermarkValue.toString).build(),
      // "payload"        -> AttributeValue.builder().s("""[{"userId":"123", "level": "user"},{"userId":"456", "level": "admin"}]""").build(),
      "payload"  -> AttributeValue.builder().s("""{"userId":{"string":"123"},"level":{"string":"user"}}""").build(),
      "schemaId" -> AttributeValue.builder().n("1").build()
    ).asJava
    response <- ZIO.attemptBlocking(
      client.putItem(
        PutItemRequest.builder().tableName(tableName).item(item).build()
      )
    )
  } yield response

object PushStreamSourceTest extends ZIOSpecDefault:
  private val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("PushStreamTests")(
    test("DetectHasRows") {
      val tableName       = "testTable"
      val primaryKeyField = "producer"
      val primaryKeyValue = "producer1"
      val watermarkField  = "timestampUTC"
      val icebergUtil     = IcebergUtil(TestDynamicSinkSettings(tableName).icebergCatalog)
      for {
        client <- PushStreamTestServices.getClient
        result <- ZIO.acquireReleaseWith(
          PushStreamTestServices.createTable(tableName, client)
        )(_ => PushStreamTestServices.deleteTable(client, tableName).orDie) { resp =>
          for {
            tables              <- PushStreamTestServices.listTables(client)
            sinkPropertyManager <- icebergUtil.getSinkTablePropertyManager
            pushStreamSource <- ZIO.succeed(
              PushStreamingSource(
                sourceTableName = tableName,
                targetTableName = tableName,
                primaryKeyFieldName = primaryKeyField,
                primaryKeyValue = primaryKeyValue,
                watermarkFieldName = watermarkField,
                dynamodbClient = client,
                sinkPropertyManager = sinkPropertyManager
              )
            )
            resp <- PushStreamTestServices.insertData(
              client,
              tableName,
              primaryKeyField,
              primaryKeyValue,
              watermarkField
            )
            changes <- pushStreamSource.hasRows(
              PushStreamWatermark(OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC))
            )
          } yield assertTrue(changes)
        }
      } yield result
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock

// TODO add it to same test object
object PushStreamSourceTest2 extends ZIOSpecDefault:
  private val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("PushStreamTests2")(
    test("DetectHasRows") {
      val tableName       = "testTable"
      val primaryKeyField = "producer"
      val primaryKeyValue = "producer1"
      val watermarkField  = "timestampUTC"
      val icebergUtil     = IcebergUtil(TestDynamicSinkSettings(tableName).icebergCatalog)
      val schema = ArcaneSchema(
        Seq(
          Field("userId", ArcaneType.StringType),
          Field("level", ArcaneType.StringType)
        )
      )
      for {
        client <- PushStreamTestServices.getClient
        result <- ZIO.acquireReleaseWith(
          PushStreamTestServices.createTable(tableName, client)
        )(_ => PushStreamTestServices.deleteTable(client, tableName).orDie) { resp =>
          for {
            tables            <- PushStreamTestServices.listTables(client)
            sinkEntityManager <- icebergUtil.getSinkEntityManager
            _ <- sinkEntityManager.createTable(
              IcebergCreateTableRequest(s"testWarehouse.testNs.$tableName", schema, true)
            )
            sinkPropertyManager <- icebergUtil.getSinkTablePropertyManager
            pushStreamSource <- ZIO.succeed(
              PushStreamingSource(
                sourceTableName = tableName,
                targetTableName = s"testWarehouse.testNs.$tableName",
                primaryKeyFieldName = primaryKeyField,
                primaryKeyValue = primaryKeyValue,
                watermarkFieldName = watermarkField,
                dynamodbClient = client,
                sinkPropertyManager = sinkPropertyManager
              )
            )
            _ <- PushStreamTestServices.insertData(
              client,
              tableName,
              primaryKeyField,
              primaryKeyValue,
              watermarkField
            )
            resp <- PushStreamTestServices.insertData(
              client,
              tableName,
              primaryKeyField,
              primaryKeyValue,
              watermarkField
            )
            changes <- pushStreamSource
              .getChanges(
                PushStreamWatermark(OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC))
              )
              .runCollect
            (rowStream, schemaStream) = changes.head
            rows <- rowStream.runCollect
          } yield assertTrue(tables.contains(tableName))
            && assertTrue(rows.length == 2)
            // row shape matches the schema (same field names, in order)
            && assertTrue(rows.head.map(_.name) == schema.map(_.name).toList)
        }
      } yield result
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
