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
  case ListType(elementType: ArcaneType, elementId: Int)
  case ObjectType
  case StructType(schema: ArcaneSchema)

  override def equals(obj: Any): Boolean = (this, obj) match {
    case (t1: ListType, t2: ListType) => t1.elementType == t2.elementType
    case (ListType, _)                => false
    case _                            => this.toString == obj.toString
  }

/** A field in the schema definition that will require indexing when converting to Iceberg
  */
trait ArcaneSchemaField:
  val name: String
  val fieldType: ArcaneType

/** A field in the schema definition that carries index information from the source that can be re-applied when
  * converting to Iceberg
  */
trait IndexedArcaneSchemaField extends ArcaneSchemaField:
  val fieldId: Int

/** Field is a case class that represents a field in ArcaneSchema
  */
final case class Field(name: String, fieldType: ArcaneType) extends ArcaneSchemaField:
  override def equals(obj: Any): Boolean = obj match
    case Field(n, t) => n.toLowerCase() == name.toLowerCase() && t == fieldType
    case _           => false

/** Field is a case class that represents a field in ArcaneSchema
  */
final case class IndexedField(name: String, fieldType: ArcaneType, fieldId: Int) extends IndexedArcaneSchemaField:
  override def equals(obj: Any): Boolean = obj match
    case IndexedField(n, t, id) => n.toLowerCase() == name.toLowerCase() && t == fieldType && id == fieldId
    case _                      => false

/** MergeKeyField represents a field used for batch merges
  */
case object MergeKeyField extends ArcaneSchemaField:
  val name: String          = "ARCANE_MERGE_KEY"
  val fieldType: ArcaneType = StringType

case class IndexedMergeKeyField(fieldId: Int) extends IndexedArcaneSchemaField:
  val name: String          = "ARCANE_MERGE_KEY"
  val fieldType: ArcaneType = StringType

/** ArcaneSchema is a type alias for a sequence of fields or structs.
  */
class ArcaneSchema(fields: Seq[ArcaneSchemaField]) extends Seq[ArcaneSchemaField]:

  /** Checks if the schema is composed of indexed fields. It is implied, but not checked for performance reasons, that a
    * schema either consists of all IndexedField instances, or none.
    * @return
    */
  def isIndexed: Boolean = fields.head match
    case IndexedField(_, _, _) => true
    case _                     => false

  /** Returns a pure schema without Arcane metadata
    * @return
    */
  def pure: ArcaneSchema = fields diff fields.filter {
    case MergeKeyField           => true
    case IndexedMergeKeyField(_) => true
    case _                       => false
  }

  def mergeKey: ArcaneSchemaField =
    val maybeMergeKey = fields.find {
      case MergeKeyField           => true
      case IndexedMergeKeyField(_) => true
      case _                       => false
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

    def addIndexedField(fieldName: String, fieldType: ArcaneType, fieldId: Int): ArcaneSchema = fieldName match
      case MergeKeyField.name => a :+ IndexedMergeKeyField(fieldId)
      case _                  => a :+ IndexedField(fieldName, fieldType, fieldId)
