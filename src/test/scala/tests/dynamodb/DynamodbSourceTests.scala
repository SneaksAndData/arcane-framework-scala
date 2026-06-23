package com.sneaksanddata.arcane.framework
package tests.dynamodb

import com.sneaksanddata.arcane.framework.services.pushstream.PushStreamingSource
import com.sneaksanddata.arcane.framework.services.pushstream.versioning.PushStreamWatermark
import software.amazon.awssdk.auth.credentials.*
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, Task, ZIO}
import zio.Console

import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.net.URI
import java.time.format.DateTimeFormatter
import scala.language.postfixOps
import tests.shared.{IcebergUtil, TestDynamicSinkSettings, TestSinkSettings}

object DynamoTestServices:
  val access_kid = "test"
  val access_secret = "test"

  def getClient: Task[DynamoDbClient] =
    ZIO.attempt(DynamoDbClient.builder().endpointOverride(URI.create("http://localhost:8000")).region(Region.US_EAST_1).credentialsProvider(
      StaticCredentialsProvider.create(AwsBasicCredentials.create(access_kid, access_secret))
    ).build())

  def createTable(tableName: String, client: DynamoDbClient): Task[CreateTableResponse] = for {
    req = CreateTableRequest.builder().tableName(tableName).keySchema(
      KeySchemaElement.builder().attributeName("userId").keyType(KeyType.HASH).build()
    ).attributeDefinitions(
      AttributeDefinition.builder()
        .attributeName("userId")
        .attributeType(ScalarAttributeType.S)
        .build()
    ).provisionedThroughput(
      ProvisionedThroughput.builder()
        .readCapacityUnits(5L)
        .writeCapacityUnits(5L)
        .build()
    ).build()
    r <- ZIO.attemptBlocking(client.createTable(req))
  } yield r

  def listTables(client: DynamoDbClient): Task[List[String]] = ZIO.attemptBlocking(
    client.listTables(ListTablesRequest.builder().build())
  ).map(_.tableNames().toArray.toList.map(_.toString))

  def deleteTable(client: DynamoDbClient, tableName: String): Task[DeleteTableResponse] =
    ZIO.attemptBlocking(client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build()))

object DynamodbSourceTests extends ZIOSpecDefault:
  private val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("DynamodbConnectionTests")(
    test("DynamoDB has changes") {
      val tableName = "testTable"
      val icebergUtil = IcebergUtil(TestDynamicSinkSettings(tableName).icebergCatalog)
      for {
        client <- DynamoTestServices.getClient
        result      <- ZIO.acquireReleaseWith(
          DynamoTestServices.createTable(tableName, client)
        )(_ => DynamoTestServices.deleteTable(client, tableName).orDie)
          { resp =>
          for {
            _ <- Console.printLine(resp.toString)
            tables <- DynamoTestServices.listTables(client)
            _ <- Console.printLine(s"----------------------\n$tables")
            sinkPropertyManager <- icebergUtil.getSinkTablePropertyManager
            pushStreamSource <- ZIO.succeed(PushStreamingSource(
              "testTable",
              "testTable",
              dynamodbClient = client, sinkPropertyManager = sinkPropertyManager
            ))
            changes <- pushStreamSource.hasRows(previousVersion = PushStreamWatermark(OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC)))
          } yield assertTrue(tables.contains(tableName)) && assertTrue(changes)
        }
      } yield result
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
