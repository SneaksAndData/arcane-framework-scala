package com.sneaksanddata.arcane.framework
package tests.dynamodb

import services.pushstream.PushStreamingSource
import services.pushstream.versioning.PushStreamWatermark
import tests.shared.{IcebergUtil, TestDynamicSinkSettings}

import com.sneaksanddata.*
import software.amazon.awssdk.auth.credentials.*
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Console, Scope, Task, ZIO}
import scala.jdk.CollectionConverters._
import java.net.URI
import java.time.format.DateTimeFormatter
import java.time.{Instant, OffsetDateTime, ZoneOffset}
import scala.language.postfixOps

object DynamoTestServices:
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
    item = (Map(
      primaryKeyField -> AttributeValue.builder().s(primaryKeyValue).build(),
      watermarkField  -> AttributeValue.builder().s(watermarkValue.toString).build(),
      "userId"        -> AttributeValue.builder().s("123").build(),
      "level"         -> AttributeValue.builder().s("user").build()
    )).asJava
    response <- ZIO.attemptBlocking(
      client.putItem(
        PutItemRequest.builder().tableName(tableName).item(item).build()
      )
    )
  } yield response

object DynamodbSourceTests extends ZIOSpecDefault:
  private val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("DynamodbConnectionTests")(
    test("DynamoDB has changes") {
      val tableName       = "testTable"
      val primaryKeyField = "producer"
      val primaryKeyValue = "producer1"
      val watermarkField  = "timestampUTC"
      val icebergUtil     = IcebergUtil(TestDynamicSinkSettings(tableName).icebergCatalog)
      for {
        client <- DynamoTestServices.getClient
        result <- ZIO.acquireReleaseWith(
          DynamoTestServices.createTable(tableName, client)
        )(_ => DynamoTestServices.deleteTable(client, tableName).orDie) { resp =>
          for {
            _                   <- Console.printLine(resp.toString)
            tables              <- DynamoTestServices.listTables(client)
            _                   <- Console.printLine(s"----------------------\n$tables")
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
            _    <- Console.printLine("after pushStream")
            resp <- DynamoTestServices.insertData(client, tableName, primaryKeyField, primaryKeyValue, watermarkField)
            _    <- Console.printLine(s"put response: $resp")
            changes <- pushStreamSource.hasRows(
              PushStreamWatermark(OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC))
            )
            _ <- Console.printLine(s"changes: $changes")
          } yield assertTrue(tables.contains(tableName)) && assertTrue(changes)
        }
      } yield result
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
