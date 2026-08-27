package com.sneaksanddata.arcane.framework
package tests.pullstream.util

import models.schemas.{ArcaneSchema, ArcaneType, Field, IndexedField}
import models.settings.sources.pullstream.PullStreamSourceSettings

import software.amazon.awssdk.auth.credentials.*
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import zio.{Random, Task, ZIO}

import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.util.UUID
import scala.jdk.CollectionConverters.*

/** Shared building blocks for the pullstream test suites.
  *
  * Every test in `tests.pullstream` follows the same pattern: create a DynamoDB table, seed it with items, exercise a
  * component, then drop the table. This object exposes reusable pieces for each step so individual tests only have to
  * describe what is unique to them.
  */
object PullStreamTestServices:
  val access_kid    = "test"
  val access_secret = "test"

  val primaryKeyField = "producer"
  val primaryKeyValue = "producer1"
  val watermarkField  = "timestampUTC"
  val idField         = "id"

  /** Identifier stored in the item's `id` attribute, which the source turns into the merge key. Production writes a
    * fresh UUID per pushed message; a deterministic one keeps assertions stable.
    */
  def defaultId(index: Int): String =
    UUID.nameUUIDFromBytes(s"arcane-pull-test-$index".getBytes(StandardCharsets.UTF_8)).toString

  /** Schema of the rows produced by [[defaultPayload]]. Kept alongside the payload so callers cannot get the two out of
    * sync.
    */
  val payloadSchema: ArcaneSchema = ArcaneSchema(
    Seq(
      Field("userId", ArcaneType.StringType),
      Field("level", ArcaneType.StringType)
    )
  )

  /** Default payload: single-row JSON array so that N inserted items produce exactly N rows. */
  def defaultPayload(index: Int): String =
    s"""[{"userId":"user-$index","level":"user"}]"""

  /** [[payloadSchema]] plus the watermark column, i.e. a sink that opts into storing the DynamoDB sort key. Note that
    * the payload deliberately never carries this field: its value comes from the item attribute.
    */
  val watermarkedPayloadSchema: ArcaneSchema =
    ArcaneSchema(payloadSchema :+ Field(watermarkField, ArcaneType.StringType))

  /** Verbatim copy of a `payload` attribute observed in the production DynamoDB table: an object root (not an array)
    * whose own `id` is a business identifier distinct from the envelope's `id`, and whose `payload` member is a nested
    * object. Neither the watermark nor the merge key appears anywhere in it.
    */
  def productionPayload(index: Int): String =
    s"""{"id":"evt_${"%03d".format(index + 1)}","payload":$productionNestedPayload}"""

  /** Nested object carried by [[productionPayload]] under its `payload` member. A string column receives it verbatim as
    * JSON text, which is how the decoder handles containers aimed at string fields.
    */
  val productionNestedPayload: String =
    """{"eventType":"Producer1Event","timestamp":"2026-08-04T12:34:56Z","source":"integration-test","message":"Hello from Avro map<string> payload"}"""

  /** Schema of the rows produced by [[productionPayload]] when a JSON pointer selects its nested `payload` member,
    * which is how a sink provisioned from that pointer declares its columns: the envelope around the pointed node
    * contributes nothing, so the payload's own `id` never surfaces and needs no rename.
    */
  val pointedPayloadSchema: ArcaneSchema = ArcaneSchema(
    Seq(
      Field("eventType", ArcaneType.StringType),
      Field("timestamp", ArcaneType.StringType),
      Field("source", ArcaneType.StringType),
      Field("message", ArcaneType.StringType)
    )
  )

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

  def createTable(tableName: String, client: DynamoDbClient): Task[CreateTableResponse] =
    val req = CreateTableRequest
      .builder()
      .tableName(tableName)
      .keySchema(
        KeySchemaElement.builder().attributeName(primaryKeyField).keyType(KeyType.HASH).build(),
        KeySchemaElement.builder().attributeName(watermarkField).keyType(KeyType.RANGE).build()
      )
      .attributeDefinitions(
        AttributeDefinition.builder().attributeName(primaryKeyField).attributeType(ScalarAttributeType.S).build(),
        AttributeDefinition.builder().attributeName(watermarkField).attributeType(ScalarAttributeType.S).build()
      )
      .provisionedThroughput(
        ProvisionedThroughput.builder().readCapacityUnits(5L).writeCapacityUnits(5L).build()
      )
      .build()
    ZIO.attemptBlocking(client.createTable(req))

  def deleteTable(client: DynamoDbClient, tableName: String): Task[DeleteTableResponse] =
    ZIO.attemptBlocking(client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build()))

  def listTables(client: DynamoDbClient): Task[List[String]] =
    ZIO
      .attemptBlocking(client.listTables(ListTablesRequest.builder().build()))
      .map(_.tableNames().asScala.toList)

  /** Inserts `count` items with monotonically increasing UTC watermarks. Pass `count = 1` when a single record is
    * enough. `payload` receives the zero-based index of each item and returns the string stored in the `payload`
    * attribute — override it to inject malformed JSON or schema-mismatched objects.
    */
  def insertMany(
      client: DynamoDbClient,
      tableName: String,
      count: Int,
      startAt: OffsetDateTime = OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC).plusHours(1),
      payload: Int => String = defaultPayload
  ): Task[Unit] =
    ZIO.foreachDiscard(0 until count) { i =>
      val ts = startAt.plusSeconds(i.toLong)
      // mirrors every attribute written by arcane-push-stream's PersistenceService, so the source is exercised
      // against the same envelope it sees in production rather than a reduced stand-in
      val item = Map(
        primaryKeyField     -> AttributeValue.builder().s(primaryKeyValue).build(),
        watermarkField      -> AttributeValue.builder().s(ts.toString).build(),
        "createdAt"         -> AttributeValue.builder().n(ts.toInstant.toEpochMilli.toString).build(),
        idField             -> AttributeValue.builder().s(defaultId(i)).build(),
        "payload"           -> AttributeValue.builder().s(payload(i)).build(),
        "schemaFingerprint" -> AttributeValue.builder().s("c1888dad68329506").build(),
        "schemaSubject"     -> AttributeValue.builder().s("producer1-event").build(),
        "schemaVersion"     -> AttributeValue.builder().n("1").build()
      ).asJava
      ZIO
        .attemptBlocking(client.putItem(PutItemRequest.builder().tableName(tableName).item(item).build()))
        .unit
    }

  /** Collision-free source table name usable across parallel test runs. */
  val genSourceTableName: Task[String] =
    Random.RandomLive.nextUUID.map(uuid => s"test_${uuid.toString.replace("-", "")}")

  def pullStreamSettings(
      sourceTableName: String,
      pointerExpression: Option[String] = None
  ): PullStreamSourceSettings =
    new PullStreamSourceSettings:
      override val tableName: String                     = sourceTableName
      override val pullIndexKey: String                  = primaryKeyField
      override val pullIndexValue: String                = PullStreamTestServices.primaryKeyValue
      override val watermarkFieldName: String            = watermarkField
      override val region: String                        = "us-east-1"
      override val endpoint: Option[String]              = None
      override val pageSize: Option[Int]                 = None
      override val jsonPointerExpression: Option[String] = pointerExpression

  /** Creates a source DynamoDB table for the duration of `use` and drops it afterward. */
  def withSourceTable[R, A](tableName: String, client: DynamoDbClient)(
      use: => ZIO[R, Throwable, A]
  ): ZIO[R, Throwable, A] =
    ZIO.acquireReleaseWith(createTable(tableName, client))(_ => deleteTable(client, tableName).orDie)(_ => use)
