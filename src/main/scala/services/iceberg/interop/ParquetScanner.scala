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

/** Streams Parquet rows from a provided Iceberg Parquet file.
  * @param icebergFile
  *   Iceberg InputFile
  */
class ParquetScanner(icebergFile: org.apache.iceberg.io.InputFile):
  private def getParquetFile: Task[InputFile] = ZIO.succeed(icebergFile).map(implicitly)
  private def getParquetSchema: Task[MessageType] = for
    file        <- getParquetFile
    parquetMeta <- ZIO.attempt(ParquetFileReader.readFooter(file, ParquetReadOptions.builder.build(), file.newStream()))
  yield parquetMeta.getFileMetaData.getSchema

  private def recordIterator: Task[Iterator[GenericRecord]] = for
    icebergSchema <- getParquetSchema
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
  yield rowsIterator

  /** Reads Parquet file schema and converts it to Iceberg schema
    * @return
    */
  def getIcebergSchema: Task[Schema] = getParquetSchema.map(implicitly)

  /** A stream of rows. Note: temporarily this returns a stream of DataRow objects. Once
    * https://github.com/SneaksAndData/arcane-framework-scala/issues/181 is resolved, conversion from GenericRecord to
    * DataRow will be removed.
    * @return
    */
  def getRows: ZStream[Any, Throwable, DataRow] = ZStream
    .fromZIO(recordIterator)
    .flatMap(ZStream.fromIterator(_))
    .map(implicitly)

object ParquetScanner:
  def apply(path: String): ParquetScanner                          = new ParquetScanner(Files.localInput(path))
  def apply(file: org.apache.iceberg.io.InputFile): ParquetScanner = new ParquetScanner(file)
