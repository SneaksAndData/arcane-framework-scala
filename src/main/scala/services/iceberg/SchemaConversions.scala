package com.sneaksanddata.arcane.framework
package services.iceberg

import models.schemas.ArcaneType.*
import models.schemas.{ArcaneSchema, ArcaneSchemaField, ArcaneType, DataCell, DataRow, Field, MergeKeyField}

import org.apache.iceberg.Schema
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.parquet.ParquetSchemaUtil
import org.apache.iceberg.types.{Type, Types}
import org.apache.parquet.schema.MessageType
import org.apache.avro.generic.GenericRecord as AvroGenericRecord
import org.apache.avro.Schema.Type as AvroType
import org.apache.avro.Schema as AvroSchema

import scala.annotation.tailrec
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
    case ListType(elementType, elementId) => Types.ListType.ofOptional(elementId, elementType)

  implicit def toIcebergSchema(schema: ArcaneSchema): Schema = new Schema(
    schema
      .foldLeft(Seq[(ArcaneSchemaField, Int, Int)]()) { (agg, e) =>
        if agg.isEmpty then agg ++ Seq((e, 0, 0))
        else
          e.fieldType match {
            case ListType(elementType, _) =>
              agg ++ Seq(
                (
                  Field(
                    name = e.name,
                    fieldType = ListType(elementType = elementType, elementId = agg.last._2 + agg.last._3 + 2)
                  ),
                  agg.last._2 + 1 + agg.last._3,
                  1
                )
              )
            case _ => agg ++ Seq((e, agg.last._2 + 1 + agg.last._3, 0))
          }
      }
      .map { (field, index, _) =>
        Types.NestedField.optional(index, field.name, field.fieldType)
      }
      .asJava
  )

  implicit def toIcebergSchemaFromFields(fields: Seq[ArcaneSchemaField]): Schema = toIcebergSchema(fields)

/** Implicit converter of Parquet schema (MessageType) to Iceberg Schema (Schema)
  */
given Conversion[org.apache.parquet.schema.MessageType, Schema] with
  override def apply(parquetSchema: MessageType): Schema = ParquetSchemaUtil.convert(parquetSchema)

given Conversion[GenericRecord, DataRow] with
  override def apply(record: GenericRecord): DataRow = record
    .struct()
    .asSchema()
    .columns()
    .asScala
    .map { nestedField =>
      DataCell(
        name = nestedField.name(),
        Type = nestedField.`type`(),
        value = record.get(nestedField.fieldId() - 1)
      )
    }
    .toList

/** Unpacks the real type from the UNION. Expects NULL to always be the first.
  * @param field
  *   AVRO field type
  * @return
  */
def unfoldAvroUnion(field: AvroSchema.Field): AvroSchema.Type =
  field.schema().getType match
    case org.apache.avro.Schema.Type.UNION =>
      field.schema().getTypes.get(1).getType
    case _ => field.schema().getType

given Conversion[AvroSchema, ArcaneSchema] with
  override def apply(avroSchema: AvroSchema): ArcaneSchema =
    ArcaneSchema(avroSchema.getFields.asScala.map { avroField =>
      Field(
        name = avroField.name(),
        fieldType = unfoldAvroUnion(avroField)
      )
    }.toSeq ++ Seq(MergeKeyField))

given Conversion[AvroType, ArcaneType] with
  override def apply(avroType: AvroType): ArcaneType = avroType match
    case org.apache.avro.Schema.Type.INT     => IntType
    case org.apache.avro.Schema.Type.LONG    => LongType
    case org.apache.avro.Schema.Type.BYTES   => ByteArrayType
    case org.apache.avro.Schema.Type.BOOLEAN => BooleanType
    case org.apache.avro.Schema.Type.STRING  => StringType
    case org.apache.avro.Schema.Type.DOUBLE  => DoubleType
    case org.apache.avro.Schema.Type.FLOAT   => FloatType
    case org.apache.avro.Schema.Type.RECORD  => throw UnsupportedOperationException("Cast from RECORD is not supported")
    case org.apache.avro.Schema.Type.ENUM    => StringType
    case org.apache.avro.Schema.Type.ARRAY   => throw UnsupportedOperationException("Cast from ARRAY is not supported")
    case org.apache.avro.Schema.Type.MAP     => throw UnsupportedOperationException("Cast from MAP is not supported")
    case org.apache.avro.Schema.Type.UNION =>
      throw UnsupportedOperationException(
        "Cast from UNION is not expected. This is a bug and it should be reported to maintainers"
      )
    case org.apache.avro.Schema.Type.FIXED => StringType // fixed-size string in AVRO, default to STRING
    case org.apache.avro.Schema.Type.NULL  => throw UnsupportedOperationException("Cast from NULL is not supported")

given Conversion[AvroGenericRecord, DataRow] with
  override def apply(record: AvroGenericRecord): DataRow = record.getSchema.getFields.asScala.map { avroField =>
    DataCell(
      name = avroField.name(),
      Type = unfoldAvroUnion(avroField),
      value = record.get(avroField.name())
    )
  }.toList

given Conversion[org.apache.iceberg.types.Type, ArcaneType] with
  final override def apply(icebergType: Type): ArcaneType = icebergType match
    case _: Types.IntegerType                             => IntType
    case _: Types.LongType                                => LongType
    case _: Types.BinaryType                              => ByteArrayType
    case _: Types.BooleanType                             => BooleanType
    case _: Types.StringType                              => StringType
    case _: Types.DateType                                => DateType
    case t: Types.TimestampType if t.shouldAdjustToUTC()  => DateTimeOffsetType
    case t: Types.TimestampType if !t.shouldAdjustToUTC() => TimestampType
    case t: Types.DecimalType                             => BigDecimalType(t.precision(), t.scale())
    case _: Types.DoubleType                              => DoubleType
    case _: Types.FloatType                               => FloatType
    case _: Types.TimeType                                => TimeType
    case t: Types.ListType                                => ListType(apply(t.elementType()), t.elementId())

given Conversion[Schema, ArcaneSchema] with
  override def apply(icebergSchema: Schema): ArcaneSchema = ArcaneSchema(
    icebergSchema.columns().asScala.map(nf => Field(name = nf.name(), fieldType = nf.`type`())).toSeq ++ Seq(
      MergeKeyField
    )
  )
