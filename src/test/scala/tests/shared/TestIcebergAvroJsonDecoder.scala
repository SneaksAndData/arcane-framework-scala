package com.sneaksanddata.arcane.framework
package tests.shared

import models.ddl.CreateTableRequest
import services.iceberg.interop.AvroJsonDecoder

import org.apache.iceberg.Schema as IcebergSchema
import org.apache.iceberg.avro.AvroSchemaUtil
import org.apache.iceberg.types.Types
import org.apache.iceberg.variants.Variant
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, ZIO}

import java.util.UUID
import scala.jdk.CollectionConverters.*

/** Integration test that decodes a push-stream style payload using an Avro schema derived from a real Iceberg table.
  */
object IcebergAvroJsonDecoder extends ZIOSpecDefault:

  private val payload =
    """
      |{
      |  "id": "evt_001",
      |  "payload": {
      |    "eventType": "Producer1Event",
      |    "timestamp": "2026-08-04T12:34:56Z",
      |    "source": "integration-test",
      |    "message": "Hello from Avro map<string> payload"
      |  }
      |}
      |""".stripMargin

  private val expectedPayload = Map(
    "eventType" -> "Producer1Event",
    "timestamp" -> "2026-08-04T12:34:56Z",
    "source"    -> "integration-test",
    "message"   -> "Hello from Avro map<string> payload"
  )

  private val expectedRawPayload =
    """{"eventType":"Producer1Event","timestamp":"2026-08-04T12:34:56Z","source":"integration-test","message":"Hello from Avro map<string> payload"}"""

  private val tableSchema = new IcebergSchema(
    Types.NestedField.optional(1, "id", Types.StringType.get()),
    Types.NestedField.optional(
      2,
      "payload",
      Types.VariantType()
      // Types.MapType.ofRequired(3, 4, Types.StringType.get(), Types.StringType.get())
    )
  )

  /** Same payload, but the sink table declares `payload` as a scalar string column. */
  private val scalarPayloadTableSchema = new IcebergSchema(
    Types.NestedField.optional(1, "id", Types.StringType.get()),
    Types.NestedField.optional(2, "payload", Types.StringType.get())
  )

  private def asStringMap(value: Any): Map[String, String] = value
    .asInstanceOf[java.util.Map[Any, Any]]
    .asScala
    .map((key, mapValue) => key.toString -> mapValue.toString)
    .toMap

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("IcebergAvroJsonDecoder")(
    test("decodes a json payload using the Avro schema of an existing Iceberg table") {
      val icebergUtil = IcebergUtil(IcebergCatalogInfo.defaultSinkSettings)
      for
        tableName       <- ZIO.succeed(s"avro_json_decoder_${UUID.randomUUID().toString.replace("-", "")}")
        entityManager   <- icebergUtil.getSinkEntityManager
        propertyManager <- icebergUtil.getSinkTablePropertyManager
        _               <- entityManager.createTable(CreateTableRequest(tableName, tableSchema, true))
        storedSchema    <- propertyManager.getTableSchema(tableName)
        avroSchema      <- ZIO.attempt(AvroSchemaUtil.convert(storedSchema, tableName))
        rows            <- ZIO.attempt(AvroJsonDecoder(avroSchema, tolerateMissingFields = false).parse(payload))
        _               <- entityManager.delete(tableName)
        row                   = rows.head
        idValue               = row.find(_.name == "id").map(_.value.toString)
        payloadValue: Variant = row.find(_.name == "payload").map(_.value).collect { case value: Variant => value }.get
        variantObject         = payloadValue.value().asObject()
      yield assertTrue(rows.size == 1)
        && assertTrue(row.map(_.name) == List("id", "payload"))
        && assertTrue(idValue.contains("evt_001"))
        // Assert arbitrary payload field
        && assertTrue(
          variantObject
            .get("source")
            .asPrimitive()
            .get()
            .toString() == "integration-test"
        )
        && assertTrue(
          variantObject
            .get("timestamp")
            .asPrimitive()
            .get()
            .toString() == "2026-08-04T12:34:56Z"
        )
    },
    test("decodes a nested object into a string column as raw json") {
      val icebergUtil = IcebergUtil(IcebergCatalogInfo.defaultSinkSettings)
      for
        tableName       <- ZIO.succeed(s"avro_json_decoder_${UUID.randomUUID().toString.replace("-", "")}")
        entityManager   <- icebergUtil.getSinkEntityManager
        propertyManager <- icebergUtil.getSinkTablePropertyManager
        _               <- entityManager.createTable(CreateTableRequest(tableName, scalarPayloadTableSchema, true))
        storedSchema    <- propertyManager.getTableSchema(tableName)
        avroSchema      <- ZIO.attempt(AvroSchemaUtil.convert(storedSchema, tableName))
        rows            <- ZIO.attempt(AvroJsonDecoder(avroSchema, tolerateMissingFields = false).parse(payload))
        _               <- entityManager.delete(tableName)
        row          = rows.head
        idValue      = row.find(_.name == "id").map(_.value.toString)
        payloadValue = row.find(_.name == "payload").map(_.value.toString)
      yield assertTrue(rows.size == 1)
        && assertTrue(idValue.contains("evt_001"))
        && assertTrue(payloadValue.contains(expectedRawPayload))
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
