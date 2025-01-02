package com.sneaksanddata.arcane.framework
package models

import models.ArcaneType.StringType

import scala.language.implicitConversions

/**
 * Types of fields in ArcaneSchema.
 */
enum ArcaneType:
  case LongType
  case ByteArrayType
  case BooleanType
  case StringType
  case DateType
  case TimestampType
  case DateTimeOffsetType
  case BigDecimalType
  case DoubleType
  case IntType
  case FloatType
  case ShortType
  case TimeType

/**
 * A field in the schema definition
 */
trait ArcaneSchemaField:
  val name: String
  val fieldType: ArcaneType

/**
 * Field is a case class that represents a field in ArcaneSchema
 */
final case class Field(name: String, fieldType: ArcaneType) extends ArcaneSchemaField

/**
 * MergeKeyField represents a field used for batch merges
 */
case object MergeKeyField extends ArcaneSchemaField:
  val name: String = "ARCANE_MERGE_KEY"
  val fieldType: ArcaneType = StringType

  
/**
 * DatePartitionField represents a field used for date partitioning
 */
case object DatePartitionField extends ArcaneSchemaField:
  val name: String = "DATE_PARTITION_KEY"
  val fieldType: ArcaneType = StringType

/**
 * ArcaneSchema is a type alias for a sequence of fields or structs.
 */
class ArcaneSchema(fields: Seq[ArcaneSchemaField]) extends Seq[ArcaneSchemaField]:
  def mergeKey: ArcaneSchemaField =
    val maybeMergeKey = fields.find {
      case MergeKeyField => true
      case _ => false
    }

    require(maybeMergeKey.isDefined, "MergeKeyField must be defined for the schema to be usable for merges")

    maybeMergeKey.get

  def apply(i: Int): ArcaneSchemaField = fields(i)

  def length: Int = fields.length

  def iterator: Iterator[ArcaneSchemaField] = fields.iterator

/**
 * Companion object for ArcaneSchema.
 */
object ArcaneSchema:
  implicit def fieldSeqToArcaneSchema(fields: Seq[ArcaneSchemaField]): ArcaneSchema = ArcaneSchema(fields)

  /**
   * Creates an empty ArcaneSchema.
   *
   * @return An empty ArcaneSchema.
   */
  def empty(): ArcaneSchema = Seq.empty
