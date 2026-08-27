package com.sneaksanddata.arcane.framework
package services.iceberg.interop

import models.schemas.{ArcaneType, DataCell, DataRow}
import services.iceberg.given_Conversion_AvroGenericRecord_DataRow

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory

import scala.jdk.CollectionConverters.*
import com.sneaksanddata.arcane.framework.exceptions.FatalStreamFailException

final class MissingFieldException(msg: String) extends FatalStreamFailException(msg)

/** Parses JSON strings/nodes into [[DataRow]]s using an Avro schema.
  *
  * Handles the Avro JSON-encoding quirk of optional fields (union `["null", T]`) by wrapping non-null values with their
  * type tag before invoking Avro's JSON decoder. See:
  *   - https://avro.apache.org/docs/current/spec.html#json_encoding
  *   - https://issues.apache.org/jira/browse/AVRO-1582
  *
  * @param schema
  *   Avro schema used to decode each record. All fields MUST have a default NULL value assigned and MUST declare NULL
  *   as the first type of their union.
  *
  * @param jsonPointerExpr
  *   Optional JSON pointer applied to each parsed root before decoding.
  *
  * @param tolerateMissingFields
  *   Optional boolean to allow missing fields in the payload. For legacy support the parser fills missing fields with
  *   Null values.
  *
  * @param decodeObjectsAsVariant
  *   Whether `record`, `map` and `array` fields are rebuilt as Iceberg variants instead of being decoded by Avro. Off
  *   by default, so that callers writing into non-variant columns keep receiving the Avro representation
  *   (`GenericRecord`/`java.util.Map`/`GenericArray`) they have always been given. Enable it when the sink declares
  *   such columns as `Types.VariantType`, since its parquet writer casts the cell value to
  *   [[org.apache.iceberg.variants.Variant]] outright.
  */
class AvroJsonDecoder(
    schema: org.apache.avro.Schema,
    jsonPointerExpr: Option[String] = None,
    tolerateMissingFields: Boolean = true,
    decodeObjectsAsVariant: Boolean = false
):
  /** Fields carried as an Iceberg variant instead of being decoded by Avro, in schema declaration order. */
  private val variantFields: Seq[org.apache.avro.Schema.Field] =
    if decodeObjectsAsVariant then schema.getFields.asScala.filter(_.isVariant).toSeq else Seq.empty

  private val variantFieldNames: Set[String] = variantFields.map(_.name()).toSet

  /** The schema Avro actually decodes with: the declared one minus any variant field.
    *
    * Avro cannot read a payload into a variant field. When the schema is derived from the sink table,
    * `AvroSchemaUtil.convert` renders a `Types.VariantType` column as a record of `{metadata: bytes, value: bytes}`
    * tagged with the `variant` logical type, so decoding a producer document against it would demand the already
    * encoded binary form. When the schema is hand-written, the field is a plain `record`/`map`/`array`, which maps to
    * `ObjectType` and therefore to a variant column as well. Either way the value is rebuilt from the raw JSON by
    * [[JsonVariantConverter]] once the remaining fields are decoded.
    */
  private val decodeSchema: org.apache.avro.Schema =
    if variantFields.isEmpty then schema
    else
      val retained = schema.getFields.asScala
        .filterNot(_.isVariant)
        .map(field => org.apache.avro.Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal()))
        .asJava
      org.apache.avro.Schema.createRecord(schema.getName, schema.getDoc, schema.getNamespace, false, retained)

  private val reader      = GenericDatumReader[GenericRecord](decodeSchema)
  private val jsonMapper  = com.fasterxml.jackson.databind.ObjectMapper()
  private val nodeFactory = JsonNodeFactory.instance

  /** The type tag Avro's JSON encoding expects for the non-null branch of an optional field.
    *
    * Named types - `record`, `enum` and `fixed` - are tagged with their full name rather than with the name of their
    * type, so a nullable nested record needs `{"tests.body": {...}}` and not `{"record": {...}}`, which Avro rejects
    * with `Unknown union branch record`. `getFullName` returns the plain type name for everything else, so it is the
    * right tag for primitives, `map` and `array` too.
    */
  private def getOptionalTypeName(optionalType: org.apache.avro.Schema): String =
    optionalType.getTypes.get(1).getFullName

  /** Avro's JSON decoder cannot read an object/array token into a `string` field. Producers that emit a structured
    * payload into a column declared as string expect the document to be stored verbatim, so serialize containers to
    * their JSON text representation before handing them over to the decoder.
    */
  private def alignValueToTargetType(value: JsonNode, targetTypeName: String): JsonNode =
    if targetTypeName == "string" && (value.isObject || value.isArray) then nodeFactory.textNode(value.toString)
    else value

  private def applyJsonPointer(node: JsonNode): JsonNode =
    jsonPointerExpr match
      case Some(pointer) => node.at(pointer)
      case None          => node

  private def getAvroCompliantNode(node: JsonNode): ObjectNode =
    val compliantNode = node.deepCopy[ObjectNode]()

    // check if any top-level nodes are missing
    // nested fields or objects with potentially missing fields are not supported -
    // this is required as AVRO requires special formatting of JSON fields that are declared as optional, see
    // https://stackoverflow.com/questions/27485580/how-to-fix-expected-start-union-got-value-number-int-when-converting-json-to-av
    // http://avro.apache.org/docs/current/spec.html#json_encoding - // https://issues.apache.org/jira/browse/AVRO-1582
    // IMPORTANT: all schema fields MUST have default value assigned to be NULL and MUST declare NULL as a first type
    // force source json to comply with AVRO requirements for optional field encoding by wrapping non-null fields in {<field_type>: <field_value > }
    // a variant field is not decoded by Avro, so it must not be validated or wrapped here either; the raw node is
    // dropped from the copy handed to the decoder and read again later, straight from the source document
    variantFieldNames.foreach(compliantNode.remove)

    decodeSchema.getFields.forEach { avroField =>
      if !avroField.hasDefaultValue then
        throw IllegalArgumentException("All fields in the schema must have default NULL value assigned")

      val jsonNodeValue = Option(compliantNode.get(avroField.name()))

      // extract field name for modified JSON
      val wrappedTypeName = getOptionalTypeName(avroField.schema())

      // create empty node
      val wrapperNode = nodeFactory.objectNode()

      // check if a node is missing
      val isFieldPresent = compliantNode.has(avroField.name())
      if !isFieldPresent && tolerateMissingFields then {
        // AVRO can fill nulls, but can't fill in missing fields - helping here
        compliantNode.set(avroField.name(), nodeFactory.nullNode())
      }
      // if node is missing and strict validation is used, throw
      if !isFieldPresent && !tolerateMissingFields then {
        throw MissingFieldException(
          s"Required field '${avroField.name()}' not present in payload"
        )
      }

      // only run this for non-null nodes
      if !jsonNodeValue.forall(_.isNull) then {
        // ignore already wrapped node
        if jsonNodeValue.flatMap(v => Option(v.get(wrappedTypeName))).isEmpty then

          // set wrapped value
          wrapperNode.set(wrappedTypeName, alignValueToTargetType(jsonNodeValue.get, wrappedTypeName))

          // create new node
          compliantNode.set(avroField.name(), wrapperNode)
      }
    }

    compliantNode

  private def decodeObjectNode(node: ObjectNode): Seq[DataRow] =
    if node.isMissingNode then {
      throw IllegalArgumentException(
        s"Applying the provided json pointer expression: `$jsonPointerExpr` resulted in an empty node"
      )
    }

    // the variant cells are rebuilt from the pre-compliant node, so each decoded row is paired with the document it
    // came from rather than being derived from the Avro objects, which no longer carry the raw JSON
    Seq(withVariantCells(decodeJson(getAvroCompliantNode(node)), node))

  /** Appends a cell per variant field, built from the source document rather than from the decoded Avro record.
    *
    * The value has to be an [[org.apache.iceberg.variants.Variant]]: such a field maps to `Types.VariantType`, whose
    * parquet writer is a `ParquetValueWriter[Variant]` and casts the cell value outright. Reading the raw JSON also
    * keeps information the Avro representation would have dropped, since an Avro `map` forces a single value type
    * across all keys.
    *
    * A field the document does not carry yields a null variant, matching the optional columns the sink declares.
    */
  private def withVariantCells(row: DataRow, source: ObjectNode): DataRow =
    if variantFields.isEmpty then row
    else
      row ++ variantFields.map(field =>
        DataCell(
          name = field.name(),
          Type = ArcaneType.ObjectType,
          value = JsonVariantConverter.toVariant(source.get(field.name()))
        )
      )

  private def decodeJson(node: ObjectNode): DataRow =
    val decoder = DecoderFactory.get().jsonDecoder(decodeSchema, node.toString)
    reader.read(null, decoder)

  /** Parses string serialized JSON — either an array root or an object root — into a sequence of [[DataRow]]s.
    */
  def parse(input: String): Seq[DataRow] =
    // TODO: instead of throw, return Either[Seq[ValidationError], Seq[DataRow]]
    // so validation errors are collected before returning e.g. multiple missing fields
    val rawJson = applyJsonPointer(jsonMapper.readTree(input))

    if rawJson.isArray then
      rawJson
        .elements()
        .asScala
        .flatMap { node =>
          if !node.isObject then throw IllegalArgumentException(s"Expected object node, got ${node.getNodeType.name()}")
          decodeObjectNode(node.asInstanceOf[ObjectNode])
        }
        .toSeq
    else if rawJson.isObject then decodeObjectNode(rawJson.asInstanceOf[ObjectNode])
    else
      throw IllegalArgumentException(
        s"Expected either array node or object node as root node of the source document. Got ${rawJson.getNodeType.name()}"
      )

object AvroJsonDecoder:
  def apply(schema: org.apache.avro.Schema): AvroJsonDecoder = new AvroJsonDecoder(schema)

  def apply(schema: org.apache.avro.Schema, tolerateMissingFields: Boolean): AvroJsonDecoder =
    new AvroJsonDecoder(schema = schema, tolerateMissingFields = tolerateMissingFields)

  def apply(schema: org.apache.avro.Schema, jsonPointerExpr: Option[String]): AvroJsonDecoder =
    new AvroJsonDecoder(schema, jsonPointerExpr)

  def apply(
      schema: org.apache.avro.Schema,
      jsonPointerExpr: Option[String],
      tolerateMissingFields: Boolean
  ): AvroJsonDecoder = new AvroJsonDecoder(schema, jsonPointerExpr, tolerateMissingFields)

  def apply(
      schema: org.apache.avro.Schema,
      jsonPointerExpr: Option[String],
      tolerateMissingFields: Boolean,
      decodeObjectsAsVariant: Boolean
  ): AvroJsonDecoder =
    new AvroJsonDecoder(schema, jsonPointerExpr, tolerateMissingFields, decodeObjectsAsVariant)

/** The payload branch of an optional field, encoded by Avro as `["null", T]`. Non-union schemas are their own. */
private def nonNullBranch(schema: org.apache.avro.Schema): org.apache.avro.Schema =
  if schema.getType == org.apache.avro.Schema.Type.UNION then
    schema.getTypes.asScala.filter(_.getType != org.apache.avro.Schema.Type.NULL).toSeq match
      case single :: Nil => single
      case _             => schema
  else schema

extension (field: org.apache.avro.Schema.Field)
  /** Whether a field is carried as an Iceberg variant rather than decoded by Avro.
    *
    * `record` covers both shapes a variant field takes: a hand-written nested record, and the
    * `{metadata: bytes, value: bytes}` record `AvroSchemaUtil.convert` emits for a `Types.VariantType` column. `map`
    * and `array` map to `ObjectType`, and therefore to a variant column, for the same reason - their contents are not
    * known until a document arrives.
    */
  def isVariant: Boolean =
    nonNullBranch(field.schema()).getType match
      case org.apache.avro.Schema.Type.RECORD | org.apache.avro.Schema.Type.MAP | org.apache.avro.Schema.Type.ARRAY =>
        true
      case _ => false
