package com.sneaksanddata.arcane.framework
package services.iceberg.interop

import logging.ZIOLogAnnotations.zlog
import models.schemas.DataRow
import services.iceberg.base.BlobScanner

import zio.stream.{ZPipeline, ZStream}
import zio.{Task, ZIO}

import java.io.File

/** JSON Scanner for files
  */
class JsonScanner(
    schema: org.apache.avro.Schema,
    filePath: String,
    jsonPointerExpr: Option[String],
    jsonArrayPointers: Map[String, Map[String, String]]
) extends BlobScanner:
  private val decoder = AvroJsonDecoder(schema, jsonPointerExpr, jsonArrayPointers)

  override protected def getRowStream: ZStream[Any, Throwable, DataRow] = ZStream
    .fromFileName(filePath)
    .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines) // assume each line a JSON object
    .flatMap(line => ZStream.from(decoder.parse(line)))

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
