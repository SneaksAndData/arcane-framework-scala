package com.sneaksanddata.arcane.framework
package tests.models.ddl

import models.ddl.CreateTableRequest

import org.apache.iceberg.types.Types
import org.apache.iceberg.Schema
import zio.test.*

import scala.jdk.CollectionConverters.*

object CreateTableRequestTests extends ZIOSpecDefault:

  private def schemaOf(fields: Types.NestedField*): Schema = Schema(fields.toList.asJava)

  private val plainSchema = schemaOf(
    Types.NestedField.optional(1, "id", Types.StringType.get())
  )

  private val variantSchema = schemaOf(
    Types.NestedField.optional(1, "id", Types.StringType.get()),
    Types.NestedField.optional(2, "payload", Types.VariantType.get())
  )

  def spec: Spec[Any, Any] = suite("CreateTableRequest")(
    test("pins a schema holding a variant column to format version 3") {
      // the staging table is created through the 3-arg apply, which carries no properties of its own:
      // without this the catalog rejects the write with "variant is not supported until v3"
      assertTrue(
        CreateTableRequest("staging", variantSchema, false).effectiveProperties ==
          Map("format-version" -> "3")
      )
    },
    test("leaves the format version alone for a schema without a variant column") {
      assertTrue(CreateTableRequest("staging", plainSchema, false).effectiveProperties.isEmpty)
    },
    test("detects a variant nested inside a struct") {
      val nested = schemaOf(
        Types.NestedField.optional(
          1,
          "wrapper",
          Types.StructType.of(Types.NestedField.optional(2, "payload", Types.VariantType.get()))
        )
      )
      assertTrue(CreateTableRequest("staging", nested, false).effectiveProperties.contains("format-version"))
    },
    test("detects a variant nested inside a list") {
      val nested = schemaOf(
        Types.NestedField.optional(1, "items", Types.ListType.ofOptional(2, Types.VariantType.get()))
      )
      assertTrue(CreateTableRequest("staging", nested, false).effectiveProperties.contains("format-version"))
    },
    test("keeps an explicitly requested format version") {
      assertTrue(
        CreateTableRequest("staging", variantSchema, false, Map("format-version" -> "4")).effectiveProperties == Map(
          "format-version" -> "4"
        )
      )
    },
    test("preserves unrelated properties alongside the format version") {
      val request = CreateTableRequest("staging", variantSchema, false, Map("comment" -> "watermark"))
      assertTrue(request.effectiveProperties == Map("comment" -> "watermark", "format-version" -> "3"))
    }
  )
