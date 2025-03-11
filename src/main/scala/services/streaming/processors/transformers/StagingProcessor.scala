package com.sneaksanddata.arcane.framework
package services.streaming.processors.transformers

import logging.ZIOLogAnnotations.zlog
import models.DataCell.schema
import models.settings.{StagingDataSettings, TablePropertiesSettings, TargetTableSettings}
import models.{ArcaneSchema, DataRow}
import services.consumers.{MergeableBatch, StagedVersionedBatch, SynapseLinkMergeBatch}
import services.lakehouse.base.{CatalogWriter, IcebergCatalogSettings}
import services.lakehouse.given_Conversion_ArcaneSchema_Schema
import services.streaming.base.{MetadataEnrichedRowStreamElement, RowGroupTransformer, StagedBatchProcessor}
import services.streaming.processors.transformers.StagingProcessor.toStagedBatch

import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import zio.stream.ZPipeline
import zio.{Chunk, Schedule, Task, ZIO, ZLayer}

import java.time.Duration

trait IndexedStagedBatches(val groupedBySchema: Iterable[StagedVersionedBatch & MergeableBatch], val batchIndex: Long)


class StagingProcessor(stagingDataSettings: StagingDataSettings,
                       tablePropertiesSettings: TablePropertiesSettings,
                       targetTableSettings: TargetTableSettings,
                       icebergCatalogSettings: IcebergCatalogSettings,
                       catalogWriter: CatalogWriter[RESTCatalog, Table, Schema])

  extends RowGroupTransformer:

  private val retryPolicy = Schedule.exponential(Duration.ofSeconds(1)) && Schedule.recurs(10)

  type OutgoingElement = StagedBatchProcessor#BatchType
  
  type IncomingElement = DataRow|Any

  override def process(toInFlightBatch: ToInFlightBatch): ZPipeline[Any, Throwable, Chunk[IncomingElement], OutgoingElement] =
    ZPipeline[Chunk[IncomingElement]]()
      .filter(_.nonEmpty)
      .mapZIO(elements =>
        val groupedBySchema = elements.withFilter(e => e.isInstanceOf[DataRow]).map(e => e.asInstanceOf[DataRow]).groupBy(row => row.schema)
        val others = elements.filterNot(e => e.isInstanceOf[DataRow])
        val applyTasks = ZIO.foreach(groupedBySchema.keys)(schema => writeDataRows(groupedBySchema(schema), schema))
        applyTasks.map(batches => (batches, others))
      )
      .zipWithIndex
      .map { case ((batches, others), index) => toInFlightBatch(batches, index, others) }

  private def writeDataRows(rows: Chunk[DataRow], arcaneSchema: ArcaneSchema): Task[StagedVersionedBatch & MergeableBatch] =
    val tableWriterEffect = zlog("Attempting to write data to staging table") *> catalogWriter.write(rows, stagingDataSettings.newStagingTableName, arcaneSchema)
    for
      table <- tableWriterEffect.tapErrorCause(cause => zlog(s"Error writing data to staging table: $cause")).retry(retryPolicy)
      batch = table.toStagedBatch(icebergCatalogSettings.namespace,
        icebergCatalogSettings.warehouse,
        arcaneSchema,
        targetTableSettings.targetTableFullName,
        tablePropertiesSettings)
    yield batch


object StagingProcessor:

  extension (table: Table) def toStagedBatch(namespace: String,
                                             warehouse: String,
                                             batchSchema: ArcaneSchema,
                                             targetName: String,
                                             tablePropertiesSettings: TablePropertiesSettings): StagedVersionedBatch & MergeableBatch =
    val batchName = table.name().split('.').last
    SynapseLinkMergeBatch(batchName, batchSchema, targetName, tablePropertiesSettings)

  def apply(stagingDataSettings: StagingDataSettings,
            tablePropertiesSettings: TablePropertiesSettings,
            targetTableSettings: TargetTableSettings,
            icebergCatalogSettings: IcebergCatalogSettings,
            catalogWriter: CatalogWriter[RESTCatalog, Table, Schema]): StagingProcessor =
    new StagingProcessor(stagingDataSettings, tablePropertiesSettings, targetTableSettings, icebergCatalogSettings, catalogWriter)


  type Environment = StagingDataSettings
    & TablePropertiesSettings
    & TargetTableSettings
    & IcebergCatalogSettings
    & CatalogWriter[RESTCatalog, Table, Schema]


  val layer: ZLayer[Environment, Nothing, StagingProcessor] =
    ZLayer {
      for
        stagingDataSettings <- ZIO.service[StagingDataSettings]
        tablePropertiesSettings <- ZIO.service[TablePropertiesSettings]
        targetTableSettings <- ZIO.service[TargetTableSettings]
        icebergCatalogSettings <- ZIO.service[IcebergCatalogSettings]
        catalogWriter <- ZIO.service[CatalogWriter[RESTCatalog, Table, Schema]]
      yield StagingProcessor(stagingDataSettings, tablePropertiesSettings, targetTableSettings, icebergCatalogSettings, catalogWriter)
    }
