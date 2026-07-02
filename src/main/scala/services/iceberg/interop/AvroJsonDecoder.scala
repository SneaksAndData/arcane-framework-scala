package com.sneaksanddata.arcane.framework
package services.iceberg.interop

import models.schemas.DataRow
import services.iceberg.given_Conversion_AvroGenericRecord_DataRow

import com.fasterxml.jackson.core.JsonPointer
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory

import scala.jdk.CollectionConverters.*

final class MissingFieldException(msg: String) extends RuntimeException(msg)

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
  * @param jsonArrayPointers
  *   Optional map of `jsonPointer -> fieldRenameMap` for exploding nested arrays into individual records (see
  *   [[JsonScanner]] for usage).
  *
  * @param tolerateMissingFields
  *   Optional boolean to allow missing fields in the payload. For legacy support the parser fills missing fields with
  *   Null values.
  */
class AvroJsonDecoder(
    schema: org.apache.avro.Schema,
    jsonPointerExpr: Option[String] = None,
    jsonArrayPointers: Map[String, Map[String, String]] = Map(),
    tolerateMissingFields: Boolean = true
):
  private val reader      = GenericDatumReader[GenericRecord](schema)
  private val jsonMapper  = com.fasterxml.jackson.databind.ObjectMapper()
  private val nodeFactory = JsonNodeFactory.instance

  private def getOptionalTypeName(optionalType: org.apache.avro.Schema): String =
    optionalType.getTypes.get(1).getType.getName

  private def applyJsonPointer(node: JsonNode): JsonNode =
    jsonPointerExpr match
      case Some(pointer) => node.at(pointer)
      case None          => node

  private def explodeJsonArray(root: JsonNode, pointerExpr: String, fieldMap: Map[String, String]): Seq[ObjectNode] =
    val rootClone = root.deepCopy[ObjectNode]()
    val arrayNode = rootClone.at(pointerExpr)

    if !arrayNode.isArray then
      throw IllegalArgumentException(
        s"Node at $pointerExpr expected to be an JsonArray, but is instead ${arrayNode.getNodeType.name()}"
      )

    val compiledPointer = JsonPointer.compile(pointerExpr)
    // assume parent is an object
    val parent = rootClone.at(compiledPointer.head()).asInstanceOf[ObjectNode]
    rootClone.remove(compiledPointer.getMatchingProperty)

    arrayNode
      .elements()
      .asScala
      .map { element =>
        val newNode = rootClone.deepCopy[ObjectNode]()

        element.fieldNames().asScala.foreach { elementFieldName =>
          newNode.set(fieldMap.getOrElse(elementFieldName, elementFieldName), element.get(elementFieldName))
        }

        getAvroCompliantNode(newNode)
      }
      .toSeq

  private def getAvroCompliantNode(node: JsonNode): ObjectNode =
    val compliantNode = node.deepCopy[ObjectNode]()

    // check if any top-level nodes are missing
    // nested fields or objects with potentially missing fields are not supported -
    // this is required as AVRO requires special formatting of JSON fields that are declared as optional, see
    // https://stackoverflow.com/questions/27485580/how-to-fix-expected-start-union-got-value-number-int-when-converting-json-to-av
    // http://avro.apache.org/docs/current/spec.html#json_encoding - // https://issues.apache.org/jira/browse/AVRO-1582
    // IMPORTANT: all schema fields MUST have default value assigned to be NULL and MUST declare NULL as a first type
    // force source json to comply with AVRO requirements for optional field encoding by wrapping non-null fields in {<field_type>: <field_value > }
    schema.getFields.forEach { avroField =>
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
          wrapperNode.set(wrappedTypeName, jsonNodeValue.get)

          // create new node
          compliantNode.set(avroField.name(), wrapperNode)
      }
    }

    compliantNode

  private def decodeObjectNode(node: ObjectNode): Seq[DataRow] =
    val avroCompliant = getAvroCompliantNode(node)

    if node.isMissingNode then {
      throw IllegalArgumentException(
        s"Applying the provided json pointer expression: `$jsonPointerExpr` resulted in an empty node"
      )
    }

    if jsonArrayPointers.isEmpty then Seq(avroCompliant).map(decodeJson)
    else {
      // first explodes array field if requested by the client
      jsonArrayPointers
        .foldLeft(Seq.empty[ObjectNode]) { case (agg, (jsonPointer, fieldMap)) =>
          if agg.isEmpty then explodeJsonArray(avroCompliant, jsonPointer, fieldMap)
          else agg.flatMap(explodeJsonArray(_, jsonPointer, fieldMap))
        }
        .map(decodeJson)
    }

  private def decodeJson(node: ObjectNode): DataRow =
    val decoder = DecoderFactory.get().jsonDecoder(schema, node.toString)
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
      jsonArrayPointers: Map[String, Map[String, String]]
  ): AvroJsonDecoder = new AvroJsonDecoder(schema, jsonPointerExpr, jsonArrayPointers)

  def apply(
      schema: org.apache.avro.Schema,
      jsonPointerExpr: Option[String],
      jsonArrayPointers: Map[String, Map[String, String]],
      tolerateMissingFields: Boolean
  ): AvroJsonDecoder = new AvroJsonDecoder(schema, jsonPointerExpr, jsonArrayPointers, tolerateMissingFields)
