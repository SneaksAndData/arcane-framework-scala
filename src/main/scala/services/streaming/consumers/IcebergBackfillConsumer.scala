package com.sneaksanddata.arcane.framework
package services.streaming.consumers

import models.app.StreamContext
import models.settings.SinkSettings
import models.{ArcaneSchema, DataRow}
import services.base.SchemaProvider
import services.consumers.StagedVersionedBatch
import services.lakehouse.{CatalogWriter, given_Conversion_ArcaneSchema_Schema}
import services.streaming.base.{BatchConsumer, BatchProcessor}
import services.streaming.consumers.IcebergBackfillConsumer.getTableName

import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import org.slf4j.{Logger, LoggerFactory}
import zio.stream.ZSink
import zio.{Chunk, ZIO, ZLayer}

/**
 * A trait that represents a streaming consumer.
 */
trait BackfillConsumer extends BatchConsumer[Chunk[DataRow]]

/**
 * A consumer that writes the data to the staging table.
 *
 * @param streamContext  The stream context.
 * @param catalogWriter  The catalog writer.
 * @param schemaProvider The schema provider.
 */
class IcebergBackfillConsumer(streamContext: StreamContext,
                               sinkSettings: SinkSettings,
                               catalogWriter: CatalogWriter[RESTCatalog, Table, Schema],
                               schemaProvider: SchemaProvider[ArcaneSchema]) extends BackfillConsumer:

  private val logger: Logger = LoggerFactory.getLogger(classOf[IcebergBackfillConsumer])

  private val tableName = getTableName(streamContext.streamId)

  /**
   * Returns the sink that consumes the batch.
   *
   * @return ZSink (stream sink for the stream graph).
   */
  def consume: ZSink[Any, Throwable, Chunk[DataRow], Any, Unit] = ZSink.foldLeftZIO(false) { (tableExists: Boolean, rows: Chunk[DataRow]) =>
    if tableExists then
      writeWithWriter(rows, tableName)
    else
      createTable(rows, tableName)
  }.map(_ => ())

  private def createTable(rows: Chunk[DataRow], name: String): ZIO[Any, Throwable, Boolean] =
    for
      arcaneSchema <- ZIO.fromFuture(implicit ec => schemaProvider.getSchema)
      table <- ZIO.fromFuture(implicit ec => catalogWriter.write(rows, name, arcaneSchema))
    yield true
    
  private def writeWithWriter(rows: Chunk[DataRow], name: String): ZIO[Any, Throwable, Boolean] =
    for
      arcaneSchema <- ZIO.fromFuture(implicit ec => schemaProvider.getSchema)
      table <- ZIO.fromFuture(implicit ec => catalogWriter.append(rows, name, arcaneSchema))
    yield true

object IcebergBackfillConsumer:
  def getTableName(streamId: String): String = s"${streamId}_backfill"


  /**
   * Factory method to create IcebergConsumer
   *
   * @param streamContext  The stream context.
   * @param sinkSettings   The stream sink settings.
   * @param catalogWriter  The catalog writer.
   * @param schemaProvider The schema provider.
   * @return The initialized IcebergConsumer instance
   */
  def apply(streamContext: StreamContext,
            sinkSettings: SinkSettings,
            catalogWriter: CatalogWriter[RESTCatalog, Table, Schema],
            schemaProvider: SchemaProvider[ArcaneSchema]): IcebergBackfillConsumer =
    new IcebergBackfillConsumer(streamContext, sinkSettings, catalogWriter, schemaProvider)

  /**
   * The required environment for the IcebergConsumer.
   */
  type Environment = SchemaProvider[ArcaneSchema]
    & CatalogWriter[RESTCatalog, Table, Schema]
    & BatchProcessor[StagedVersionedBatch, Boolean]
    & StreamContext
    & SinkSettings

  /**
   * The ZLayer that creates the IcebergConsumer.
   */
  val layer: ZLayer[Environment, Nothing, BackfillConsumer] =
    ZLayer {
      for
        streamContext <- ZIO.service[StreamContext]
        sinkSettings <- ZIO.service[SinkSettings]
        catalogWriter <- ZIO.service[CatalogWriter[RESTCatalog, Table, Schema]]
        schemaProvider <- ZIO.service[SchemaProvider[ArcaneSchema]]
      yield IcebergBackfillConsumer(streamContext, sinkSettings, catalogWriter, schemaProvider)
    }
