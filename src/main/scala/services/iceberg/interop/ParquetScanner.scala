package com.sneaksanddata.arcane.framework
package services.iceberg.interop

import logging.ZIOLogAnnotations.zlogStream
import models.schemas.{ArcaneSchema, DataRow}
import services.iceberg.{given_Conversion_GenericRecord_DataRow, given_Conversion_MessageType_Schema}
import services.iceberg.interop.given

import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.parquet.GenericParquetReaders
import org.apache.iceberg.parquet.Parquet
import org.apache.iceberg.{Files, Schema}
import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.InputFile
import org.apache.parquet.schema.MessageType
import zio.stream.ZStream
import zio.{Task, ZIO}

import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions

class ParquetScanner(icebergFile: org.apache.iceberg.io.InputFile):
  private def getParquetFile: Task[InputFile] = ZIO.succeed(icebergFile).map(implicitly)
  private def getSchema: Task[MessageType] = for
    file        <- getParquetFile
    parquetMeta <- ZIO.attempt(ParquetFileReader.readFooter(file, ParquetReadOptions.builder.build(), file.newStream()))
  yield parquetMeta.getFileMetaData.getSchema

  def getRows: ZStream[Any, Throwable, DataRow] = ZStream
    .fromZIO(for
      icebergSchema <- getSchema
      rowsIterator <- ZIO.attemptBlocking(
        Parquet
          .read(icebergFile)
          .project(icebergSchema)
          .createReaderFunc(schema =>
            GenericParquetReaders.buildReader(
              icebergSchema,
              schema
            )
          )
          .build[GenericRecord]()
          .iterator()
          .asScala
      )
    yield rowsIterator)
    .flatMap(ZStream.fromIterator(_))
    .map(implicitly)

object ParquetScanner:
  def apply(path: String): ParquetScanner                          = new ParquetScanner(Files.localInput(path))
  def apply(file: org.apache.iceberg.io.InputFile): ParquetScanner = new ParquetScanner(file)
