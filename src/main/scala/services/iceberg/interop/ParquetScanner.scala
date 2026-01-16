package com.sneaksanddata.arcane.framework
package services.iceberg.interop

import logging.ZIOLogAnnotations.{zlog, zlogStream}
import models.schemas.{ArcaneSchema, DataRow}
import services.iceberg.{given_Conversion_GenericRecord_DataRow, given_Conversion_MessageType_Schema}
import services.iceberg.interop.given
import extensions.ZExtensions.*

import com.sneaksanddata.arcane.framework.services.iceberg.base.BlobScanner
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.parquet.GenericParquetReaders
import org.apache.iceberg.mapping.MappingUtil
import org.apache.iceberg.parquet.Parquet
import org.apache.iceberg.{Files, Schema}
import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.InputFile
import org.apache.parquet.schema.MessageType
import zio.stream.ZStream
import zio.{Task, ZIO}

import java.io.File
import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions

/** Streams Parquet rows from a provided Iceberg Parquet file.
  * @param icebergFile
  *   Iceberg InputFile
  */
class ParquetScanner(icebergFile: org.apache.iceberg.io.InputFile, useNameMapping: Boolean) extends BlobScanner:
  private def getParquetFile: Task[InputFile] = ZIO.succeed(icebergFile).map(implicitly)
  private def getParquetSchema: Task[MessageType] = for
    file        <- getParquetFile
    parquetMeta <- ZIO.attempt(ParquetFileReader.readFooter(file, ParquetReadOptions.builder.build(), file.newStream()))
  yield parquetMeta.getFileMetaData.getSchema

  private def recordIterator: Task[Iterator[GenericRecord]] = (for
    icebergSchema <- getParquetSchema
    nameMapping   <- ZIO.succeed(if useNameMapping then Some(MappingUtil.create(icebergSchema)) else None)
    iteratorBuilder <- ZIO.attempt(
      Parquet
        .read(icebergFile)
        .project(icebergSchema)
        .createReaderFunc(schema =>
          GenericParquetReaders.buildReader(
            icebergSchema,
            schema
          )
        )
    )
  yield nameMapping match {
    case Some(mapping) => iteratorBuilder.withNameMapping(mapping)
    case None          => iteratorBuilder
  }).flatMap(builder => ZIO.attemptBlocking(builder.build[GenericRecord]().iterator().asScala))

  /** Reads Parquet file schema and converts it to Iceberg schema
    * @return
    */
  def getIcebergSchema: Task[Schema] = getParquetSchema.map(implicitly)

  override def cleanup: Task[Unit] = for
    file <- ZIO.succeed(new File(icebergFile.location()))
    _    <- zlog("Row stream finished for file %s, deleting", icebergFile.location())
    _ <- ZIO
      .attemptBlockingIO(file.delete())
      .orDie // require deletion to succeed, to avoid risking filling up the temp storage
  yield ()

  override protected def getRowStream: ZStream[Any, Throwable, DataRow] = ZStream
    .fromZIO(recordIterator)
    .flatMap(ZStream.fromIterator(_))
    .map(implicitly)

object ParquetScanner:
  def apply(path: String, useNameMapping: Boolean): ParquetScanner =
    new ParquetScanner(Files.localInput(path), useNameMapping)
  def apply(file: org.apache.iceberg.io.InputFile, useNameMapping: Boolean): ParquetScanner =
    new ParquetScanner(file, useNameMapping)
