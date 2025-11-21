package com.sneaksanddata.arcane.framework
package services.iceberg.interop

import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode}
import logging.ZIOLogAnnotations.zlog
import models.schemas.DataRow
import services.iceberg.base.BlobScanner
import services.iceberg.given_Conversion_AvroGenericRecord_DataRow

import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import zio.stream.{ZPipeline, ZStream}
import zio.{Task, ZIO}

import java.io.File

class JsonScanner(schema: org.apache.avro.Schema, filePath: String) extends BlobScanner:
  private val reader      = GenericDatumReader[GenericRecord](schema)
  private val jsonMapper  = com.fasterxml.jackson.databind.ObjectMapper()
  private val nodeFactory = JsonNodeFactory.instance

  private def getOptionalTypeName(optionalType: org.apache.avro.Schema): String =
    optionalType.getTypes.get(1).getType.getName

  private def parseJsonLine(line: String): GenericRecord =
    val rawJson  = jsonMapper.readTree(line)
    val safeJson = rawJson.deepCopy[ObjectNode]()
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

      val jsonNodeValue = Option(rawJson.get(avroField.name()))

      // extract field name for modified JSON
      val wrappedTypeName = getOptionalTypeName(avroField.schema())

      // create empty node
      val wrapperNode = nodeFactory.objectNode()

      // check if a node is missing
      if !rawJson.has(avroField.name()) then
        // AVRO can fill nulls, but can't fill in missing fields - helping here
        safeJson.set(avroField.name(), nodeFactory.nullNode())

      // only run this for non-null nodes
      if !jsonNodeValue.forall(_.isNull) then
        wrapperNode.set(wrappedTypeName, jsonNodeValue.get)

        // create new node
        safeJson.set(avroField.name(), wrapperNode)

    }

    val decoder = DecoderFactory.get().jsonDecoder(schema, safeJson.toString)

    reader.read(null, decoder)

  override protected def getRowStream: ZStream[Any, Throwable, DataRow] = ZStream
    .fromFileName(filePath)
    .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines) // assume each line a JSON object
    .map(parseJsonLine)

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
    filePath = path
  )

  def parseSchema(schemaStr: String): org.apache.avro.Schema = org.apache.avro.Schema.Parser().parse(schemaStr)
