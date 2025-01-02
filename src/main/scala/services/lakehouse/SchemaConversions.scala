package com.sneaksanddata.arcane.framework
package services.lakehouse

import models.{ArcaneSchema, ArcaneSchemaField, ArcaneType}
import models.ArcaneType.*

import org.apache.iceberg.Schema
import org.apache.iceberg.types.Types

import scala.language.implicitConversions
import scala.jdk.CollectionConverters.*

/**
 * Implicit conversions from ArcaneType to Iceberg schema types
 */
object SchemaConversions:
  implicit def toIcebergType(arcaneType: ArcaneType): org.apache.iceberg.types.Type = arcaneType match
    case IntType => Types.IntegerType.get()
    case LongType => Types.LongType.get()
    case ByteArrayType => Types.BinaryType.get()
    case BooleanType => Types.BooleanType.get()
    case StringType => Types.StringType.get()
    case DateType => Types.DateType.get()
    case TimestampType => Types.TimestampType.withoutZone()
    case DateTimeOffsetType => Types.TimestampType.withZone()
    case BigDecimalType => Types.DecimalType.of(30, 6)
    case DoubleType => Types.DoubleType.get()
    case FloatType => Types.FloatType.get()
    case ShortType => Types.IntegerType.get()
    case TimeType => Types.TimeType.get()

  implicit def toIcebergSchema(schema: ArcaneSchema): Schema = new Schema(
    schema
      .zipWithIndex
      .map {
        (field, index) => 
          Types.NestedField.optional(index, field.name, field.fieldType)
      }.asJava
  )

  implicit def toIcebergSchemaFromFields(fields: Seq[ArcaneSchemaField]): Schema = toIcebergSchema(fields)
