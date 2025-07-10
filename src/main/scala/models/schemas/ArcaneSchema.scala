package com.sneaksanddata.arcane.framework
package models.schemas

import ArcaneType.StringType
import models.*
import services.base.CanAdd

import scala.language.implicitConversions

/** Types of fields in ArcaneSchema.
  */
enum ArcaneType:
  case LongType
  case ByteArrayType
  case BooleanType
  case StringType
  case DateType
  case TimestampType
  case DateTimeOffsetType
  case BigDecimalType(precision: Int, scale: Int)
  case DoubleType
  case IntType
  case FloatType
  case ShortType
  case TimeType

/** A field in the schema definition
  */
trait ArcaneSchemaField:
  val name: String
  val fieldType: ArcaneType

/** Field is a case class that represents a field in ArcaneSchema
  */
final case class Field(name: String, fieldType: ArcaneType) extends ArcaneSchemaField:
  override def equals(obj: Any): Boolean = obj match
    case Field(n, t) => n.toLowerCase() == name.toLowerCase() && t == fieldType
    case _           => false

/** MergeKeyField represents a field used for batch merges
  */
case object MergeKeyField extends ArcaneSchemaField:
  val name: String          = "ARCANE_MERGE_KEY"
  val fieldType: ArcaneType = StringType

/** ArcaneSchema is a type alias for a sequence of fields or structs.
  */
class ArcaneSchema(fields: Seq[ArcaneSchemaField]) extends Seq[ArcaneSchemaField]:
  def mergeKey: ArcaneSchemaField =
    val maybeMergeKey = fields.find {
      case MergeKeyField => true
      case _             => false
    }

    require(maybeMergeKey.isDefined, "MergeKeyField must be defined for the schema to be usable for merges")

    maybeMergeKey.get

  def apply(i: Int): ArcaneSchemaField = fields(i)

  def length: Int = fields.length

  def iterator: Iterator[ArcaneSchemaField] = fields.iterator

/** Companion object for ArcaneSchema.
  */
object ArcaneSchema:
  implicit def fieldSeqToArcaneSchema(fields: Seq[ArcaneSchemaField]): ArcaneSchema = ArcaneSchema(fields)

  /** Creates an empty ArcaneSchema.
    *
    * @return
    *   An empty ArcaneSchema.
    */
  def empty(): ArcaneSchema = Seq.empty

  /** Converts a schema to an SQL column expression.
    */
  extension (schema: ArcaneSchema) def toColumnsExpression: String = s"(${schema.map(f => f.name).mkString(", ")})"

  /** Gets the fields that are missing in the target schema.
    *
    * @param batches
    *   The schema to compare.
    * @param targetSchema
    *   The target schema.
    * @return
    *   The missing fields.
    */
  extension (targetSchema: ArcaneSchema)
    def getMissingFields(batches: ArcaneSchema): Seq[ArcaneSchemaField] =
      batches.filter { batchField =>
        !targetSchema.exists(targetField =>
          targetField.name.toLowerCase() == batchField.name.toLowerCase()
            && targetField.fieldType == batchField.fieldType
        )
      }

given NamedCell[ArcaneSchemaField] with
  extension (field: ArcaneSchemaField) def name: String = field.name

/** Required typeclass implementation
  */
given CanAdd[ArcaneSchema] with
  extension (a: ArcaneSchema)
    def addField(fieldName: String, fieldType: ArcaneType): ArcaneSchema = fieldName match
      case MergeKeyField.name => a :+ MergeKeyField
      case _                  => a :+ Field(fieldName, fieldType)
