package com.sneaksanddata.arcane.framework
package services.iceberg

import models.schemas.ArcaneType.*
import models.schemas.{ArcaneSchema, ArcaneSchemaField, ArcaneType}

import org.apache.iceberg.Schema
import org.apache.iceberg.parquet.ParquetSchemaUtil
import org.apache.iceberg.types.Types
import org.apache.parquet.schema.MessageType

import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions

/** Implicit conversions from ArcaneType to Iceberg schema types
  */
object SchemaConversions:
  implicit def toIcebergType(arcaneType: ArcaneType): org.apache.iceberg.types.Type = arcaneType match
    case IntType                          => Types.IntegerType.get()
    case LongType                         => Types.LongType.get()
    case ByteArrayType                    => Types.BinaryType.get()
    case BooleanType                      => Types.BooleanType.get()
    case StringType                       => Types.StringType.get()
    case DateType                         => Types.DateType.get()
    case TimestampType                    => Types.TimestampType.withoutZone()
    case DateTimeOffsetType               => Types.TimestampType.withZone()
    case BigDecimalType(precision, scale) => Types.DecimalType.of(precision, scale)
    case DoubleType                       => Types.DoubleType.get()
    case FloatType                        => Types.FloatType.get()
    case ShortType                        => Types.IntegerType.get()
    case TimeType                         => Types.TimeType.get()

  implicit def toIcebergSchema(schema: ArcaneSchema): Schema = new Schema(
    schema.zipWithIndex.map { (field, index) =>
      Types.NestedField.optional(index, field.name, field.fieldType)
    }.asJava
  )

  implicit def toIcebergSchemaFromFields(fields: Seq[ArcaneSchemaField]): Schema = toIcebergSchema(fields)


/**
 * Implicit converter of Parquet schema (MessageType) to Iceberg Schema (Schema)
 */
given Conversion[org.apache.parquet.schema.MessageType, Schema] with
  override def apply(parquetSchema: MessageType): Schema = ParquetSchemaUtil.convert(parquetSchema)
