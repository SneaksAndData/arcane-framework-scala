package com.sneaksanddata.arcane.framework
package services.iceberg.interop

import logging.ZIOLogAnnotations.zlog
import models.schemas.{ArcaneType, DataCell, DataRow}
import services.iceberg.base.BlobScanner
import services.iceberg.{given_Conversion_AvroGenericRecord_DataRow, given_Conversion_AvroType_ArcaneType}

import org.apache.avro.data.Json
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.iceberg.Schema
import zio.stream.{ZPipeline, ZStream}
import zio.{Task, ZIO}

import java.io.File
import scala.jdk.CollectionConverters.*

class JsonScanner(schema: org.apache.avro.Schema, filePath: String) extends BlobScanner:
  private val reader = GenericDatumReader[GenericRecord](schema)

  private def parseJsonLine(line: String): GenericRecord =
    val decoder = DecoderFactory.get().jsonDecoder(schema, line)
    reader.read(null, decoder)


  override protected def getRowStream: ZStream[Any, Throwable, DataRow] = ZStream.fromFileName(filePath)
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
    schema=schema,
    filePath = path
  )

  def parseSchema(schemaStr: String): org.apache.avro.Schema = org.apache.avro.Schema.Parser().parse(schemaStr)


