package com.sneaksanddata.arcane.framework
package tests.iceberg.interop

import models.schemas.DataCell
import services.iceberg.interop.{AvroJsonDecoder, MissingFieldException}

import org.apache.avro.{Schema, SchemaBuilder}
import zio.test.*

/** Unit coverage for [[AvroJsonDecoder]].
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

  /** Avro hands back `Utf8` rather than `String`, so normalise before comparing. */
  private def cells(row: Seq[DataCell]): Map[String, String] =
    row.map(cell => cell.name -> Option(cell.value).map(_.toString).orNull).toMap

  def spec: Spec[Any, Any] = suite("AvroJsonDecoder")(
    suite("decoding")(
      test("decodes the root fields into their own columns") {
        val decoder = new AvroJsonDecoder(schema = schemaOf("id", "payload"), tolerateMissingFields = false)

        val rows = decoder.parse(productionPayload)

        assertTrue(rows.size == 1) && assertTrue(cells(rows.head)("id") == "evt_001")
      },
      test("emits one row per element when the document root is an array") {
        val decoder = new AvroJsonDecoder(schema = schemaOf("id"), tolerateMissingFields = false)

        val rows = decoder.parse("""[{"id":"evt_001"},{"id":"evt_002"}]""")

        assertTrue(rows.map(cells) == Seq(Map("id" -> "evt_001"), Map("id" -> "evt_002")))
      },
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
      }
    ),
    suite("missing field handling")(
      test("reports a field the document does not supply") {
        val decoder = new AvroJsonDecoder(
          schema = schemaOf("id", "payload", "absentColumn"),
          tolerateMissingFields = false
        )

        val error = scala.util.Try(decoder.parse(productionPayload)).failed.get

        assertTrue(error.isInstanceOf[MissingFieldException]) && assertTrue(
          error.getMessage.contains("Required field 'absentColumn' not present in payload")
        )
      },
      test("fills a field the document does not supply with null when tolerant") {
        val decoder = new AvroJsonDecoder(
          schema = schemaOf("id", "payload", "absentColumn"),
          tolerateMissingFields = true
        )

        val row = cells(decoder.parse(productionPayload).head)

        assertTrue(row("absentColumn") == null) && assertTrue(row("id") == "evt_001")
      }
    )
  )
