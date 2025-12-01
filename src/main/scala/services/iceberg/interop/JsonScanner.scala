package com.sneaksanddata.arcane.framework
package services.iceberg.interop

import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode}
import logging.ZIOLogAnnotations.zlog
import models.schemas.DataRow
import services.iceberg.base.BlobScanner
import services.iceberg.given_Conversion_AvroGenericRecord_DataRow

import com.fasterxml.jackson.core.JsonPointer
import com.fasterxml.jackson.databind.JsonNode
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import zio.stream.{ZPipeline, ZStream}
import zio.{Task, ZIO}

import java.io.File
import scala.jdk.CollectionConverters.*

class JsonScanner(
    schema: org.apache.avro.Schema,
    filePath: String,
    jsonPointerExpr: Option[String],
    jsonArrayPointers: Map[String, Map[String, String]]
) extends BlobScanner:
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
      throw IllegalArgumentException(s"Node at $pointerExpr expected to be an JsonArray, but it is not")

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
    // nested fields or objects with potentially missing fields are not supported
    // this is required as AVRO requires special formatting of JSON fields that are declared as optional, see
    // https://stackoverflow.com/questions/27485580/how-to-fix-expected-start-union-got-value-number-int-when-converting-json-to-av
    // http://avro.apache.org/docs/current/spec.html#json_encoding
    // https://issues.apache.org/jira/browse/AVRO-1582
    // IMPORTANT: all schema fields MUST have default value assigned to be NULL and MUST declare NULL as a first type
    // force source json to comply with AVRO requirements for optional field encoding by wrapping non-null fields in {<field_type>: <field_value>}
    schema.getFields.forEach { avroField =>
      if !avroField.hasDefaultValue then
        throw IllegalArgumentException("All fields in the schema must have default NULL value assigned")

      val jsonNodeValue = Option(compliantNode.get(avroField.name()))

      // extract field name for modified JSON
      val wrappedTypeName = getOptionalTypeName(avroField.schema())

      // create empty node
      val wrapperNode = nodeFactory.objectNode()

      // check if a node is missing
      if !compliantNode.has(avroField.name()) then
        // AVRO can fill nulls, but can't fill in missing fields - helping here
        compliantNode.set(avroField.name(), nodeFactory.nullNode())

      // only run this for non-null nodes
      if !jsonNodeValue.forall(_.isNull) then
        // ignore already wrapped node
        if jsonNodeValue.flatMap(v => Option(v.get(wrappedTypeName))).isEmpty then

          // set wrapped value
          wrapperNode.set(wrappedTypeName, jsonNodeValue.get)

          // create new node
          compliantNode.set(avroField.name(), wrapperNode)

    }

    compliantNode

  private def decodeJson(node: ObjectNode): DataRow =
    val decoder = DecoderFactory.get().jsonDecoder(schema, node.toString)
    reader.read(null, decoder)

  private def parseJsonLine(line: String): Seq[DataRow] =
    val rawJson       = applyJsonPointer(jsonMapper.readTree(line))
    val avroCompliant = getAvroCompliantNode(rawJson.asInstanceOf[ObjectNode])

    if rawJson.isMissingNode then {
      throw IllegalArgumentException(
        s"Applying the provided json pointer expression: `$jsonPointerExpr` resulted in an empty node"
      )
    }

    if jsonArrayPointers.isEmpty then Seq(avroCompliant).map(decodeJson)
    else
      // first explode array fields if requested by the client
      jsonArrayPointers
        .foldLeft(Seq.empty[ObjectNode]) { case (agg, (jsonPointer, fieldMap)) =>
          if agg.isEmpty then explodeJsonArray(avroCompliant, jsonPointer, fieldMap)
          else agg.flatMap(explodeJsonArray(_, jsonPointer, fieldMap))
        }
        .map(decodeJson)

  override protected def getRowStream: ZStream[Any, Throwable, DataRow] = ZStream
    .fromFileName(filePath)
    .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines) // assume each line a JSON object
    .flatMap(line => ZStream.from(parseJsonLine(line)))

  override def cleanup: Task[Unit] = for
    file <- ZIO.succeed(new File(filePath))
    _    <- zlog("Row stream finished for file %s, deleting", filePath)
    _ <- ZIO
      .attemptBlockingIO(file.delete())
      .orDie // require deletion to succeed, to avoid risking filling up the temp storage
  yield ()

object JsonScanner:
  def apply(path: String, schema: org.apache.avro.Schema): JsonScanner = new JsonScanner(
    schema = schema,
    filePath = path,
    None,
    Map()
  )

  def apply(path: String, schema: org.apache.avro.Schema, jsonPointerExpr: Option[String]): JsonScanner =
    new JsonScanner(
      schema = schema,
      filePath = path,
      jsonPointerExpr,
      Map()
    )

  def apply(
      path: String,
      schema: org.apache.avro.Schema,
      jsonPointerExpr: Option[String],
      jsonArrayPointers: Map[String, Map[String, String]]
  ): JsonScanner =
    new JsonScanner(
      schema = schema,
      filePath = path,
      jsonPointerExpr,
      jsonArrayPointers
    )

  def parseSchema(schemaStr: String): org.apache.avro.Schema = org.apache.avro.Schema.Parser().parse(schemaStr)
