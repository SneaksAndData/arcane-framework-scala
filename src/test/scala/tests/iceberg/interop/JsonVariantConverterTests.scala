package com.sneaksanddata.arcane.framework
package tests.iceberg.interop

import models.schemas.{ArcaneType, DataCell}
import services.iceberg.interop.{AvroJsonDecoder, JsonVariantConverter}

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.iceberg.avro.AvroSchemaUtil
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.parquet.{GenericParquetReaders, GenericParquetWriter}
import org.apache.iceberg.parquet.Parquet
import org.apache.iceberg.types.Types
import org.apache.iceberg.variants.{PhysicalType, Variant, VariantPrimitive}
import org.apache.iceberg.{Files, PartitionSpec, Schema}
import zio.test.*

import java.nio.file.{Files as JavaFiles}
import scala.jdk.CollectionConverters.*

/** Covers the JSON -> Iceberg variant conversion and, most importantly, that the produced values survive the real
  * parquet writer.
  *
  * A column typed `ObjectType` becomes `Types.VariantType`, whose parquet writer is a `ParquetValueWriter[Variant]` and
  * casts the cell value outright. Before [[JsonVariantConverter]] existed the decoder handed it the Avro object
  * (`GenericRecord`/`Map`/`GenericArray`) and the write died with a `ClassCastException`, so the round-trip test below
  * is the one that actually pins the behaviour down.
  */
object JsonVariantConverterTests extends ZIOSpecDefault:

  private val mapper = ObjectMapper()

  private def variantOf(json: String): Variant = JsonVariantConverter.toVariant(mapper.readTree(json))

  private def primitive(value: org.apache.iceberg.variants.VariantValue): Any =
    value.asPrimitive().asInstanceOf[VariantPrimitive[?]].get()

  /** A nested payload as produced by push-stream, declared as an Avro `record` so it lands in a variant column. */
  private val nestedRecordSchema =
    """{
      |  "type": "record", "name": "event", "namespace": "tests",
      |  "fields": [
      |    { "name": "id", "type": ["null", "string"], "default": null },
      |    { "name": "payload", "type": ["null", {
      |        "type": "record", "name": "body", "namespace": "tests",
      |        "fields": [
      |          { "name": "eventType", "type": ["null", "string"], "default": null },
      |          { "name": "sequence",  "type": ["null", "int"],    "default": null }
      |        ]
      |      }], "default": null }
      |  ]
      |}""".stripMargin

  def spec: Spec[Any, Any] = suite("JsonVariantConverter")(
    suite("conversion")(
      test("keeps nested objects addressable by field name") {
        val variant = variantOf("""{"eventType":"Producer1Event","nested":{"depth":2}}""")
        val obj     = variant.value().asObject()

        assertTrue(
          primitive(obj.get("eventType")) == "Producer1Event",
          primitive(obj.get("nested").asObject().get("depth")) == 2.toByte
        )
      },
      test("preserves arrays, including their element order") {
        val variant = variantOf("""{"tags":["a","b","c"]}""")
        val array   = variant.value().asObject().get("tags").asArray()

        assertTrue(
          array.numElements() == 3,
          primitive(array.get(0)) == "a",
          primitive(array.get(2)) == "c"
        )
      },
      test("narrows integral numbers instead of promoting them to double") {
        val variant = variantOf("""{"small":7,"big":5000000000,"fraction":1.5}""")
        val obj     = variant.value().asObject()

        assertTrue(
          obj.get("small").asPrimitive().`type`() == PhysicalType.INT8,
          obj.get("big").asPrimitive().`type`() == PhysicalType.INT64,
          primitive(obj.get("fraction")) == 1.5
        )
      },
      test("records an absent document as a null variant rather than throwing") {
        assertTrue(JsonVariantConverter.toVariant(null).value().asPrimitive().`type`() == PhysicalType.NULL)
      }
    ),
    suite("decoder integration")(
      test("emits a Variant value for a nested record field") {
        val decoder = new AvroJsonDecoder(
          schema = new org.apache.avro.Schema.Parser().parse(nestedRecordSchema),
          tolerateMissingFields = false
        )

        val rows        = decoder.parse("""{"id":"evt_001","payload":{"eventType":"Producer1Event","sequence":3}}""")
        val payloadCell = rows.head.find(_.name == "payload").get

        // the cast is what the parquet writer performs, so asserting the concrete type is the point of the test
        val obj = payloadCell.value.asInstanceOf[Variant].value().asObject()

        assertTrue(
          payloadCell.Type == ArcaneType.ObjectType,
          primitive(obj.get("eventType")) == "Producer1Event",
          primitive(obj.get("sequence")) == 3.toByte
        )
      },
      test("leaves scalar cells untouched") {
        val decoder = new AvroJsonDecoder(
          schema = new org.apache.avro.Schema.Parser().parse(nestedRecordSchema),
          tolerateMissingFields = false
        )

        val rows = decoder.parse("""{"id":"evt_001","payload":{"eventType":"a","sequence":1}}""")

        assertTrue(rows.head.find(_.name == "id").get.value.toString == "evt_001")
      },
      test("decodes a variant column declared the way the sink schema renders it") {
        // AvroSchemaUtil.convert turns a Types.VariantType column into a {metadata, value} record of bytes, which is
        // what stream-pull hands the decoder; the producer document must still decode against it
        val sinkSchema = AvroSchemaUtil.convert(
          new Schema(
            Types.NestedField.optional(1, "id", Types.StringType.get()),
            Types.NestedField.optional(2, "payload", Types.VariantType.get())
          ),
          "events"
        )

        val rows =
          new AvroJsonDecoder(schema = sinkSchema, tolerateMissingFields = false)
            .parse("""{"id":"evt_001","payload":{"eventType":"Producer1Event","sequence":3}}""")

        val obj = rows.head.find(_.name == "payload").get.value.asInstanceOf[Variant].value().asObject()

        assertTrue(
          rows.head.find(_.name == "id").get.value.toString == "evt_001",
          primitive(obj.get("eventType")) == "Producer1Event"
        )
      },
      test("records a missing nested document as a null variant") {
        val decoder = new AvroJsonDecoder(
          schema = new org.apache.avro.Schema.Parser().parse(nestedRecordSchema),
          tolerateMissingFields = true
        )

        val rows = decoder.parse("""{"id":"evt_001"}""")

        assertTrue(
          rows.head.find(_.name == "payload").get.value.asInstanceOf[Variant].value().asPrimitive().`type`()
            == PhysicalType.NULL
        )
      }
    ),
    suite("parquet round-trip")(
      test("writes and reads back a variant column through the iceberg writer") {
        val schema = new Schema(
          Types.NestedField.optional(1, "id", Types.StringType.get()),
          Types.NestedField.optional(2, "payload", Types.VariantType.get())
        )

        val record = GenericRecord.create(schema)
        record.setField("id", "evt_001")
        record.setField("payload", variantOf("""{"eventType":"Producer1Event","sequence":3}"""))

        val target = JavaFiles.createTempFile("variant-roundtrip", ".parquet").toFile
        target.delete()

        val writer = Parquet
          .writeData(Files.localOutput(target))
          .schema(schema)
          .createWriterFunc(GenericParquetWriter.create)
          .overwrite()
          .withSpec(PartitionSpec.unpartitioned())
          .build[GenericRecord]()

        try writer.write(List(record).asJava)
        finally writer.close()

        val reader = Parquet
          .read(Files.localInput(target))
          .project(schema)
          .createReaderFunc(fileSchema => GenericParquetReaders.buildReader(schema, fileSchema))
          .build[GenericRecord]()

        val readBack =
          try reader.iterator().asScala.toList
          finally reader.close()

        target.delete()

        val payload = readBack.head.getField("payload").asInstanceOf[Variant].value().asObject()

        assertTrue(
          writer.toDataFile.recordCount() == 1L,
          readBack.size == 1,
          readBack.head.getField("id").toString == "evt_001",
          primitive(payload.get("eventType")) == "Producer1Event"
        )
      }
    )
  )
