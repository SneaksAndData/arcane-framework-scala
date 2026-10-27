package com.sneaksanddata.arcane.framework
package services.pullstream

import models.app.PluginStreamContext
import models.schemas.{ArcaneSchema, DataRow, MergeKeyField, VersionField}
import models.settings.TableNaming.parts
import models.settings.sources.pullstream.PullStreamSourceSettings
import logging.ZIOLogAnnotations.zlog

import com.sneaksanddata.arcane.framework.migrations.base.{CurrentMigration, Migration, SinkMigrationIdentifier}
import extensions.ZExtensions.combineWith
import models.settings.sources.modification.{DataRowModification, FrozenSurrogateMergeKey, FrozenSurrogateVersion}
import services.base.{InsertUpdateDeleteSource, SchemaProvider, StreamingSource}
import services.iceberg.base.SinkPropertyManager
import services.iceberg.given_Conversion_Schema_MergeableArcaneSchema
import services.iceberg.interop.AvroJsonDecoder
import services.pullstream.migrations.PullStreamArcaneMergeKeyMigration
import services.pullstream.versioning.PullStreamWatermark
import services.streaming.base.StructuredZStream

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.avro.Schema as AvroSchema
import org.apache.iceberg.avro.AvroSchemaUtil
import org.apache.iceberg.types.Types
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
  */

class PullStreamingSource(
    settings: PullStreamSourceSettings,
    dynamodbClient: DynamoDbClient,
    sinkPropertyManager: SinkPropertyManager,
    targetTableFullName: String,
    pageSize: Option[Int],
    modifications: Seq[DataRowModification] = Seq.empty
) extends InsertUpdateDeleteSource(modifications):

  override def definedMigrations: Map[SinkMigrationIdentifier, Seq[Migration]] =
    Map(
      CurrentMigration -> (super.definedMigrations(CurrentMigration) ++ Seq(
        new PullStreamArcaneMergeKeyMigration()
      ))
    )

  /** Schema provided by Push Stream - modifications should not be applied twice.
    */
  override def applySchemaModification(schema: ArcaneSchema, modification: DataRowModification): Task[ArcaneSchema] =
    ZIO.succeed(schema)

  private val pushPayloadFieldName: String = "payload"
  private val pushIdFieldName: String      = "id"
  private val formatter                    = DateTimeFormatter.ISO_OFFSET_DATE_TIME
  private val listPageSize                 = pageSize.getOrElse(PullStreamingSource.defaultPageSize)

  /** Names under which the DynamoDB envelope's `id` and watermark attribute are merged into the document, in place of
    * their literal names: `payload` may itself carry fields called `id` or named after the watermark column, and a
    * literal merge would silently overwrite them. The prefix guarantees the merged fields never collide with anything
    * `payload` can legally contain.
    */
  private val dynamoIdFieldName: String = s"${PullStreamingSource.dynamoAttributePrefix}$pushIdFieldName"
  private val dynamoVersionFieldName: String =
    s"${PullStreamingSource.dynamoAttributePrefix}${settings.versionFieldName}"

  /** Column used to order concurrent versions of the same merge key during a merge. Now that the literal watermark
    * column is never populated from the DynamoDB envelope (see `dynamoVersionFieldName`), `ARCANE_VERSION` is the only
    * column consistently carrying that ordering value.
    */
  val versionFieldName: String = VersionField.name

  /** Name of the target Iceberg table, without warehouse and namespace. Note that `settings.tableName` refers to the
    * DynamoDB table holding the pushed payloads and must never be used to address the Iceberg sink.
    */
  private val targetTableName: String = targetTableFullName.parts.name

  override protected def getPrimaryKey: Task[FrozenSurrogateMergeKey] =
    ZIO.succeed(FrozenSurrogateMergeKey(Set(dynamoIdFieldName.toLowerCase)))

  override protected val getVersionField: Task[FrozenSurrogateVersion] =
    ZIO.succeed(FrozenSurrogateVersion(dynamoVersionFieldName))

  override protected def getSourceSchema: Task[ArcaneSchema] =
    this.sinkPropertyManager.getTableSchema(targetTableName).map(implicitly)

  override def getShards(rangeStart: WatermarkType, rangeEnd: WatermarkType): ZStream[Any, Throwable, ShardMetadata] =
    ZStream.empty

  override def deleteShards(prefix: String): Task[Unit] = ZIO.unit

  override type ShardMetadata = String
  override type WatermarkType = PullStreamWatermark

  override def empty: SchemaType = ArcaneSchema.empty()

  private def buildQueryGetChanges(latestVersion: PullStreamWatermark): QueryRequest =
    val exprNames = Map(
      "#pk" -> settings.pullIndexKey,
      "#wm" -> settings.versionFieldName
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
      "#wm" -> settings.versionFieldName
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
      "#wm" -> settings.versionFieldName
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
    (avro: AvroSchema, iceberg: org.apache.iceberg.Schema, pointer: Option[String])
  ] =
    for
      icebergSchema <- this.sinkPropertyManager.getTableSchema(targetTableName)
      pointer       <- resolveJsonPointer
    yield {
      // ARCANE_MERGE_KEY and ARCANE_VERSION are synthesized by the row modifications from the `dattr__`-prefixed
      // envelope fields (see responseStream); they are never present in `payload` itself, so under strict decoding
      // they must be excluded from the schema the decoder validates against. The unpruned sink schema is still
      // returned, since the staging table must declare both columns for their values to be written.
      val excludedColumnNames = Set(MergeKeyField.name, VersionField.name)
      val payloadColumns = icebergSchema
        .columns()
        .asScala
        .filterNot(column => excludedColumnNames.exists(_.equalsIgnoreCase(column.name())))

      // The `dattr__` fields are never declared by the sink, since they exist only to carry the DynamoDB envelope's
      // `id`/watermark into the row modifications; the decoder must be told about them explicitly.
      val withEnvelopeColumns = payloadColumns ++ Seq(
        Types.NestedField.optional(
          payloadColumns.map(_.fieldId()).maxOption.getOrElse(0) + 1,
          dynamoIdFieldName,
          Types.StringType.get()
        ),
        Types.NestedField.optional(
          payloadColumns.map(_.fieldId()).maxOption.getOrElse(0) + 2,
          dynamoVersionFieldName,
          Types.LongType.get()
        )
      )

      (
        AvroSchemaUtil.convert(org.apache.iceberg.Schema(withEnvelopeColumns.asJava), targetTableName),
        icebergSchema,
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

  /** Parse the dynamodb query response into DataRows.
    *
    * Only `payload` holds producer data; the remaining item attributes are envelope metadata. `id` and the watermark
    * attribute are merged into the document under `dattr__`-prefixed field names before it is handed to the decoder, so
    * a single Avro decode produces `ARCANE_MERGE_KEY`/`ARCANE_VERSION`'s source fields alongside the rest of the row,
    * instead of a second pass rebuilding every row to append them. The prefix avoids a name collision with a payload
    * that happens to carry its own `id` or watermark-named field: merging under the literal name would silently
    * overwrite that payload data.
    *
    * The item iterable is chunked explicitly rather than at ZIO's default of 4096: a chunk is decoded as a unit, so
    * letting a whole page land in one chunk would hold every decoded row of that page in memory at once. A page is up
    * to `listPageSize` items of raw JSON, and decoding amplifies that into DataRows, which is where the peak sits.
    */
  private def responseStream(
      queryResponse: QueryResponse,
      avroSchema: AvroSchema,
      jsonPointer: Option[String]
  ): ZStream[Any, Throwable, DataRow] =
    // nested documents land in Types.VariantType columns on the sink, whose parquet writer expects a Variant.
    // The pointer is applied here, before the envelope fields are merged in, rather than passed to the decoder: a
    // pointer into a nested member must receive the envelope fields at the pointed node, not at the document root.
    val decoder = new AvroJsonDecoder(schema = avroSchema, tolerateMissingFields = false)

    ZStream
      .fromIterable(queryResponse.items().asScala, PullStreamingSource.decodeChunkSize)
      .map { item =>
        val attributes = item.asScala
        val payload    = attributes(pushPayloadFieldName).s()
        val watermarkMillis =
          OffsetDateTime.parse(attributes(settings.versionFieldName).s(), formatter).toInstant.toEpochMilli
        val rootNode = PullStreamingSource.jsonMapper.readTree(payload)
        val pointedNode = jsonPointer.fold(rootNode)(rootNode.at) match
          case obj: ObjectNode => obj
          case other =>
            throw IllegalArgumentException(s"Expected object node, got ${other.getNodeType.name()}")
        pointedNode.put(dynamoIdFieldName, attributes(pushIdFieldName).s())
        pointedNode.put(dynamoVersionFieldName, watermarkMillis)
        pointedNode.toString
      }
      .mapZIO(mergedDocument => ZIO.attempt(decoder.parse(mergedDocument)))
      .flatMap(ZStream.fromIterable)

  /** Gets the changes in the database since the given version.
    *
    * @param previousVersion
    *   The version to fetch changes from.
    * @return
    *   An effect containing the changes in the database since the given version and the latest observed version.
    */
  def getChanges(previousVersion: PullStreamWatermark): ZStream[Any, Throwable, StructuredZStream] =
    ZStream.fromZIO(getSchemaInfo.combineWith(allModifications)).map {
      case ((avroSchema, icebergSchema, jsonPointer), mods) =>
        val rowStream: ZStream[Any, Throwable, DataRow] =
          paginatedQuery(buildQueryGetChanges(previousVersion))
            .flatMap(response => responseStream(response, avroSchema, jsonPointer))
            .mapChunks(rowChunk => applyDataRowModifications(rowChunk, mods))
            .map(dropDynamoAttributeFields)
        (rowStream, icebergSchema)
    }

  /** Removes the `dattr__`-prefixed DynamoDB envelope fields from a decoded row.
    *
    * They exist solely to feed `FrozenSurrogateMergeKey`/`FrozenSurrogateVersion` without colliding with payload field
    * names (see responseStream), and are never declared as sink columns, so they must not reach the writer.
    */
  private def dropDynamoAttributeFields(row: DataRow): DataRow =
    row.filterNot(_.name.startsWith(PullStreamingSource.dynamoAttributePrefix))

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

  /** Shared parser used to merge `id`/the watermark attribute into each document before it reaches the decoder. */
  private val jsonMapper = new ObjectMapper()

  /** Prefix for the field names the DynamoDB envelope's `id` and watermark attribute are merged under. `payload` may
    * legally carry its own `id` field or one named after the watermark column; this prefix keeps the merged copies from
    * colliding with (and overwriting) that payload data.
    */
  val dynamoAttributePrefix: String = "dattr__"

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

  def apply(
      settings: PullStreamSourceSettings,
      dynamodbClient: DynamoDbClient,
      sinkPropertyManager: SinkPropertyManager,
      targetTableFullName: String,
      pageSize: Option[Int],
      modifications: Seq[DataRowModification] = Seq.empty
  ): PullStreamingSource =
    new PullStreamingSource(
      settings,
      dynamodbClient,
      sinkPropertyManager,
      targetTableFullName,
      pageSize,
      modifications
    )

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
        settings.pageSize,
        context.source.modifications.rowModifications
      )
    }
