package com.sneaksanddata.arcane.framework
package tests.iceberg

import models.schemas.ArcaneType.{BigDecimalType, ListType, StringType}
import models.schemas.{ArcaneSchema, IndexedMergeKeyField, MergeKeyField}
import services.iceberg.{given_Conversion_ArcaneSchema_Schema, given_Conversion_Schema_ArcaneSchema, inferMergeKeyIndex}

import org.apache.iceberg.Schema
import org.apache.iceberg.types.Types
import org.scalatest.Inspectors.forAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should

import scala.jdk.CollectionConverters.*
import scala.language.postfixOps

// for inspiration https://github.com/apache/iceberg/blob/c588d8d516500290a26619e5b48bdcab6ee37b6e/docs/docs/java-api-quickstart.md?plain=1#L127
class SchemaConversionsTests extends AnyFlatSpec with Matchers {
  it should "convert Iceberg schema to Arcane Schema for all supported types" in {
    val iceberg = new Schema(
      Types.NestedField.optional(0, "level", Types.StringType.get()),
      Types.NestedField.optional(1, "event_time", Types.TimestampType.withZone()),
      Types.NestedField.optional(2, "message", Types.StringType.get()),
      Types.NestedField.optional(3, "event_value_int", Types.IntegerType.get()),
      Types.NestedField.optional(4, "event_value_long", Types.LongType.get()),
      Types.NestedField.optional(5, "event_value_bytes", Types.BinaryType.get()),
      Types.NestedField.optional(6, "event_value_bool", Types.BooleanType.get()),
      Types.NestedField.optional(7, "event_value_date", Types.DateType.get()),
      Types.NestedField.optional(8, "event_value_timestampntz", Types.TimestampType.withoutZone()),
      Types.NestedField.optional(9, "event_value_timestamptz", Types.TimestampType.withZone()),
      Types.NestedField.optional(10, "event_value_bigdecimal", Types.DecimalType.of(16, 4)),
      Types.NestedField.optional(11, "event_value_double", Types.DoubleType.get()),
      Types.NestedField.optional(12, "event_value_float", Types.FloatType.get()),
      Types.NestedField.optional(13, "event_value_time", Types.TimeType.get()),
      Types.NestedField.optional(14, "call_stack", Types.ListType.ofOptional(15, Types.StringType.get())),
      Types.NestedField.optional(
        16,
        "call_tree",
        new Schema(
          Types.NestedField.optional(17, "caller", Types.StringType.get()),
          Types.NestedField.optional(18, "call_time", Types.TimestampType.withZone())
        ).asStruct()
      )
    )

    val mergeKeyIndex = inferMergeKeyIndex(iceberg.columns().getLast)

    val arcaneSchema: ArcaneSchema = implicitly(iceberg)
    (
      arcaneSchema.length should be(iceberg.columns().size + 1),
      arcaneSchema.reverse.head should be(IndexedMergeKeyField(mergeKeyIndex)),
      arcaneSchema
        .find(f => f.name == "event_value_bigdecimal")
        .map(f => f.fieldType == BigDecimalType(16, 4)) should be(Some(true))
    )
  }

  it should "support two Nested fields in a sequence" in {
    val iceberg = new Schema(
      Types.NestedField.optional(0, "level", Types.StringType.get()),
      Types.NestedField.optional(1, "event_time", Types.TimestampType.withZone()),
      Types.NestedField.optional(2, "call_stack_1", Types.ListType.ofOptional(3, Types.StringType.get())),
      Types.NestedField.optional(4, "call_stack_2", Types.ListType.ofOptional(5, Types.StringType.get())),
      Types.NestedField.optional(6, "event_time_2", Types.TimestampType.withZone())
    )

    val arcaneSchema: ArcaneSchema = implicitly(iceberg)
    val mergeKeyIndex              = inferMergeKeyIndex(iceberg.columns().getLast)

    (
      arcaneSchema.length should be(iceberg.columns().size() + 1),
      arcaneSchema.reverse.head should be(IndexedMergeKeyField(mergeKeyIndex)),
      arcaneSchema.find(f => f.name == "call_stack_1").map(f => f.fieldType == ListType(StringType, 3)) should be(
        Some(true)
      ),
      arcaneSchema.find(f => f.name == "call_stack_2").map(f => f.fieldType == ListType(StringType, 5)) should be(
        Some(true)
      )
    )
  }

  it should "convert from Iceberg to ArcaneSchema and back" in {
    forAll(
      Seq(
//        new Schema(
//          Types.NestedField.optional(0, "level", Types.StringType.get()),
//          Types.NestedField.optional(1, "event_time", Types.TimestampType.withZone()),
//          Types.NestedField.optional(2, "call_stack_1", Types.ListType.ofOptional(3, Types.StringType.get())),
//          Types.NestedField.optional(4, "call_stack_2", Types.ListType.ofOptional(5, Types.StringType.get())),
//          Types.NestedField.optional(6, "event_time_2", Types.TimestampType.withZone())
//        ),
//        new Schema(
//          Types.NestedField.optional(0, "call_stack_1", Types.ListType.ofOptional(1, Types.StringType.get()))
//        ),
//        new Schema(
//          Types.NestedField.optional(0, "level", Types.StringType.get()),
//          Types.NestedField.optional(1, "call_stack_1", Types.ListType.ofOptional(2, Types.StringType.get())),
//          Types.NestedField.optional(3, "call_stack_2", Types.ListType.ofOptional(4, Types.StringType.get()))
//        ),
//        new Schema(
//          Types.NestedField.optional(0, "level", Types.StringType.get()),
//          Types.NestedField.optional(1, "call_stack_1", Types.ListType.ofOptional(2, Types.StringType.get())),
//          Types.NestedField.optional(3, "event_time", Types.TimestampType.withZone()),
//          Types.NestedField.optional(4, "call_stack_2", Types.ListType.ofOptional(5, Types.StringType.get())),
//          Types.NestedField.optional(6, "event_time_2", Types.TimestampType.withZone())
//        ),
        new Schema(
          Types.NestedField.optional(0, "level", Types.StringType.get()),
          Types.NestedField.optional(1, "call_stack_1", Types.ListType.ofOptional(2, Types.StringType.get())),
          Types.NestedField.optional(
            3,
            "nested",
            new Schema(
              Types.NestedField.optional(4, "nested_level", Types.StringType.get()),
              Types.NestedField
                .optional(5, "nested_call_stack_1", Types.ListType.ofOptional(6, Types.StringType.get())),
              Types.NestedField.optional(7, "nested_call_stack_2", Types.ListType.ofOptional(8, Types.StringType.get()))
            ).asStruct()
          ),
          Types.NestedField.optional(9, "nested_call_stack_2", Types.ListType.ofOptional(10, Types.StringType.get())),
          Types.NestedField.optional(11, "nested_event_time_2", Types.TimestampType.withZone())
        )
      )
    ) { iceberg =>

      val mergeKeyIndex              = inferMergeKeyIndex(iceberg.columns().getLast)
      val arcaneSchema: ArcaneSchema = implicitly(iceberg)
      val pure: ArcaneSchema         = arcaneSchema diff Seq(IndexedMergeKeyField(mergeKeyIndex))
      val iceberg2: Schema           = implicitly(pure)

      (
        iceberg2.columns().size() should be(iceberg.columns().size()),
        iceberg2.idToName().asScala should equal(iceberg.idToName().asScala)
      )
    }
  }
}
