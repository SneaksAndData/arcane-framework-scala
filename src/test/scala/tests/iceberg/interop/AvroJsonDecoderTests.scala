package com.sneaksanddata.arcane.framework
package tests.iceberg.interop

import models.schemas.DataCell
import services.iceberg.interop.{AvroJsonDecoder, MissingFieldException}

import org.apache.avro.{Schema, SchemaBuilder}
import zio.test.*

/** Unit coverage for [[AvroJsonDecoder]], focused on how nested documents are flattened into columns.
  *
  * These run without a catalog, unlike `IcebergAvroJsonDecoder`, which exercises the same decoder against a schema
  * derived from a live Iceberg table.
  */
object AvroJsonDecoderTests extends ZIOSpecDefault:

  /** Every field is a nullable union with a null default, which is what the decoder requires and what
    * `AvroSchemaUtil.convert` produces for an Iceberg table of optional columns.
    */
  private def schemaOf(fields: String*): Schema =
    fields
      .foldLeft(SchemaBuilder.record("payload").namespace("tests").fields())((assembler, field) =>
        assembler.optionalString(field)
      )
      .endRecord()

  /** The `payload` attribute of a real push-stream DynamoDB item: an object root whose `payload` member is a nested
    * object, and whose own `id` is a business identifier.
    */
  private val productionPayload =
    """{"id":"evt_001","payload":{"eventType":"Producer1Event","timestamp":"2026-08-04T12:34:56Z","source":"integration-test","message":"Hello from Avro map<string> payload"}}"""

  private val hoistPayloadPointer = Map("/payload" -> Map("id" -> "push_event_id"))

  /** Avro hands back `Utf8` rather than `String`, so normalise before comparing. */
  private def cells(row: Seq[DataCell]): Map[String, String] =
    row.map(cell => cell.name -> Option(cell.value).map(_.toString).orNull).toMap

  def spec: Spec[Any, Any] = suite("AvroJsonDecoder")(
    suite("hoisting nested nodes")(
      test("lifts the members of a nested object into their own columns") {
        val decoder = new AvroJsonDecoder(
          schema = schemaOf("push_event_id", "eventType", "timestamp", "source", "message"),
          jsonArrayPointers = hoistPayloadPointer,
          tolerateMissingFields = false
        )

        val rows = decoder.parse(productionPayload)

        // an object yields exactly one row, unlike an array which yields one per element
        assertTrue(rows.size == 1) && assertTrue(
          cells(rows.head) == Map(
            "push_event_id" -> "evt_001",
            "eventType"     -> "Producer1Event",
            "timestamp"     -> "2026-08-04T12:34:56Z",
            "source"        -> "integration-test",
            "message"       -> "Hello from Avro map<string> payload"
          )
        )
      },
      test("renames a root field, not only the hoisted ones") {
        val decoder = new AvroJsonDecoder(
          schema = schemaOf("push_event_id", "eventType"),
          jsonArrayPointers = Map("/payload" -> Map("id" -> "push_event_id")),
          tolerateMissingFields = false
        )

        // `id` lives at the root, so a rename map restricted to nested fields could never reach it
        val row = decoder.parse("""{"id":"evt_001","payload":{"eventType":"Producer1Event"}}""").head

        assertTrue(cells(row) == Map("push_event_id" -> "evt_001", "eventType" -> "Producer1Event"))
      },
      test("leaves fields absent from the rename map under their original names") {
        val decoder = new AvroJsonDecoder(
          schema = schemaOf("id", "eventType"),
          jsonArrayPointers = Map("/payload" -> Map()),
          tolerateMissingFields = false
        )

        val row = decoder.parse("""{"id":"evt_001","payload":{"eventType":"Producer1Event"}}""").head

        assertTrue(cells(row) == Map("id" -> "evt_001", "eventType" -> "Producer1Event"))
      },
      test("emits one row per element when the nested node is an array, repeating the root fields") {
        val decoder = new AvroJsonDecoder(
          schema = schemaOf("batchId", "itemId"),
          jsonArrayPointers = Map("/items" -> Map()),
          tolerateMissingFields = false
        )

        val rows = decoder.parse("""{"batchId":"b1","items":[{"itemId":"i1"},{"itemId":"i2"}]}""")

        assertTrue(rows.size == 2) && assertTrue(
          rows.map(cells) == Seq(
            Map("batchId" -> "b1", "itemId" -> "i1"),
            Map("batchId" -> "b1", "itemId" -> "i2")
          )
        )
      },
      test("hoists every element when the document root is an array") {
        val decoder = new AvroJsonDecoder(
          schema = schemaOf("id", "eventType"),
          jsonArrayPointers = Map("/payload" -> Map()),
          tolerateMissingFields = false
        )

        val rows = decoder.parse(
          """[{"id":"evt_001","payload":{"eventType":"A"}},{"id":"evt_002","payload":{"eventType":"B"}}]"""
        )

        assertTrue(
          rows.map(cells) == Seq(Map("id" -> "evt_001", "eventType" -> "A"), Map("id" -> "evt_002", "eventType" -> "B"))
        )
      },
      test("applies several pointers in succession") {
        val decoder = new AvroJsonDecoder(
          schema = schemaOf("id", "eventType", "region"),
          jsonArrayPointers = Map("/payload" -> Map(), "/meta" -> Map()),
          tolerateMissingFields = false
        )

        val row = decoder.parse("""{"id":"e1","payload":{"eventType":"A"},"meta":{"region":"eu"}}""").head

        assertTrue(cells(row) == Map("id" -> "e1", "eventType" -> "A", "region" -> "eu"))
      },
      test("lets a hoisted field win over a root field of the same name") {
        val decoder = new AvroJsonDecoder(
          schema = schemaOf("source"),
          jsonArrayPointers = Map("/payload" -> Map()),
          tolerateMissingFields = false
        )

        val row = decoder.parse("""{"source":"envelope","payload":{"source":"nested"}}""").head

        assertTrue(cells(row) == Map("source" -> "nested"))
      },
      test("rejects a pointer that resolves to a scalar") {
        val decoder = new AvroJsonDecoder(
          schema = schemaOf("id"),
          jsonArrayPointers = Map("/payload" -> Map()),
          tolerateMissingFields = false
        )

        val error = scala.util.Try(decoder.parse("""{"id":"evt_001","payload":"not-a-container"}""")).failed.get

        assertTrue(error.isInstanceOf[IllegalArgumentException]) && assertTrue(
          error.getMessage.contains("expected to be a JsonArray or JsonObject, but is instead STRING")
        )
      }
    ),
    suite("missing field handling")(
      test("validates required fields only after hoisting has moved them to the root") {
        val decoder = new AvroJsonDecoder(
          schema = schemaOf("push_event_id", "eventType", "timestamp", "source", "message"),
          jsonArrayPointers = hoistPayloadPointer,
          tolerateMissingFields = false
        )

        // none of the schema's fields exist at the root of the raw document, so validating before hoisting would
        // reject the payload for the very fields it is about to receive
        assertTrue(decoder.parse(productionPayload).size == 1)
      },
      test("still reports a field that hoisting does not supply") {
        val decoder = new AvroJsonDecoder(
          schema = schemaOf("push_event_id", "eventType", "absentColumn"),
          jsonArrayPointers = hoistPayloadPointer,
          tolerateMissingFields = false
        )

        val error = scala.util.Try(decoder.parse(productionPayload)).failed.get

        assertTrue(error.isInstanceOf[MissingFieldException]) && assertTrue(
          error.getMessage.contains("Required field 'absentColumn' not present in payload")
        )
      },
      test("fills a field that hoisting does not supply with null when tolerant") {
        val decoder = new AvroJsonDecoder(
          schema = schemaOf("push_event_id", "eventType", "absentColumn"),
          jsonArrayPointers = hoistPayloadPointer,
          tolerateMissingFields = true
        )

        val row = cells(decoder.parse(productionPayload).head)

        assertTrue(row("absentColumn") == null) && assertTrue(row("push_event_id") == "evt_001")
      }
    ),
    suite("without hoisting")(
      test("stores a nested object verbatim in a string column") {
        val decoder = new AvroJsonDecoder(schema = schemaOf("id", "payload"), tolerateMissingFields = false)

        val row = decoder.parse(productionPayload).head

        // the decoder cannot read an object token into a string field, so it serializes the container first
        assertTrue(
          cells(row)("payload").toString ==
            """{"eventType":"Producer1Event","timestamp":"2026-08-04T12:34:56Z","source":"integration-test","message":"Hello from Avro map<string> payload"}"""
        )
      },
      test("decodes from the node addressed by the json pointer expression") {
        val decoder = new AvroJsonDecoder(
          schema = schemaOf("eventType"),
          jsonPointerExpr = Some("/payload"),
          tolerateMissingFields = false
        )

        val row = decoder.parse(productionPayload).head

        assertTrue(cells(row)("eventType") == "Producer1Event")
      },
      test("combines the json pointer expression with hoisting applied to the addressed node") {
        val decoder = new AvroJsonDecoder(
          schema = schemaOf("outer", "inner"),
          jsonPointerExpr = Some("/envelope"),
          jsonArrayPointers = Map("/nested" -> Map()),
          tolerateMissingFields = false
        )

        val row = decoder.parse("""{"envelope":{"outer":"o","nested":{"inner":"i"}}}""").head

        assertTrue(cells(row) == Map("outer" -> "o", "inner" -> "i"))
      }
    )
  )
