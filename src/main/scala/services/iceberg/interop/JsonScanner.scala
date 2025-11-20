package com.sneaksanddata.arcane.framework
package services.iceberg.interop

import models.schemas.{ArcaneType, DataCell, DataRow}
import services.iceberg.{given_Conversion_AvroGenericRecord_DataRow, given_Conversion_AvroType_ArcaneType}

import org.apache.avro.data.Json
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.iceberg.Schema
import zio.Task
import zio.stream.{ZPipeline, ZStream}

import java.io.File
import scala.jdk.CollectionConverters.*

class JsonScanner(schema: org.apache.avro.Schema, filePath: String):
  private val reader = GenericDatumReader[GenericRecord](schema)

  private def parseJsonLine(line: String): GenericRecord =
    val decoder = DecoderFactory.get().jsonDecoder(schema, line)
    reader.read(null, decoder)


  def getRows: ZStream[Any, Throwable, DataRow] = ZStream.fromFileName(filePath)
    .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines) // assume each line a JSON object
    .map(parseJsonLine)

object JsonScanner:
  def apply(path: String, schema: org.apache.avro.Schema): JsonScanner = new JsonScanner(
    schema=schema,
    filePath = path
  )

  def parseSchema(schemaStr: String): org.apache.avro.Schema = org.apache.avro.Schema.Parser().parse(schemaStr)


