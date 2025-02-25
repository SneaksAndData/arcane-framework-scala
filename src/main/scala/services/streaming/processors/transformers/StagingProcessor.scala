package com.sneaksanddata.arcane.framework
package services.streaming.processors.transformers

import logging.ZIOLogAnnotations.zlog
import models.settings.{StagingDataSettings, TablePropertiesSettings}
import models.{ArcaneSchema, DataRow, MergeKeyField}
import services.consumers.{StagedVersionedBatch, SynapseLinkMergeBatch}
import services.lakehouse.base.IcebergCatalogSettings
import services.lakehouse.{CatalogWriter, given_Conversion_ArcaneSchema_Schema}
import services.streaming.base.{BatchProcessor, MetadataEnrichedRowStreamElement, RowGroupTransformer, ToInFlightBatch}

import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import zio.stream.ZPipeline
import zio.{Chunk, Schedule, Task, ZIO, ZLayer}

import java.time.format.DateTimeFormatter
import java.time.{Duration, ZoneOffset, ZonedDateTime}
import java.util.UUID
import com.sneaksanddata.arcane.framework.models.settings.ArchiveTableSettings
import com.sneaksanddata.arcane.framework.models.settings.TargetTableSettings
import com.sneaksanddata.arcane.framework.models.DataCell.schema
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.StagingProcessor.toStagedBatch

trait IndexedStagedBatches(groupedBySchema: Iterable[StagedVersionedBatch], batchIndex: Long)


class StagingProcessor(stagingDataSettings: StagingDataSettings,
                       tablePropertiesSettings: TablePropertiesSettings,
                       targetTableSettings: TargetTableSettings,
                       icebergCatalogSettings: IcebergCatalogSettings,
                       archiveTableSettings: ArchiveTableSettings,
                       catalogWriter: CatalogWriter[RESTCatalog, Table, Schema])

  extends RowGroupTransformer:

  private val retryPolicy = Schedule.exponential(Duration.ofSeconds(1)) && Schedule.recurs(10)

  override def process(toInFlightBatch: ToInFlightBatch): ZPipeline[Any, Throwable, Chunk[IncomingElement], OutgoingElement] =
    ZPipeline[Chunk[IncomingElement]]()
      .mapZIO(elements =>
        val groupedBySchema = elements.withFilter(e => e.isDataRow(e)).map(e => e.toDataRow(e)).groupBy(row => row.schema)
        val others = elements.filterNot(e => e.isDataRow(e))
        val applyTasks = ZIO.foreach(groupedBySchema.keys)(schema => writeDataRows(groupedBySchema(schema), schema))
        applyTasks.map(batches => (batches, others))
      )
      .zipWithIndex
      .map { case ((batches, others), index) => toInFlightBatch(batches, index, others) }

  private def writeDataRows(rows: Chunk[DataRow], arcaneSchema: ArcaneSchema): Task[StagedVersionedBatch] =
    val tableWriterEffect =
        zlog("Attempting to write data to staging table") *>
        ZIO.fromFuture(implicit ec => catalogWriter.write(rows, stagingDataSettings.newStagingTableName, arcaneSchema))
    for
      table <- tableWriterEffect.retry(retryPolicy)
      batch = table.toStagedBatch(icebergCatalogSettings.namespace,
        icebergCatalogSettings.warehouse,
        arcaneSchema,
        targetTableSettings.targetTableFullName,
        archiveTableSettings.fullName,
        tablePropertiesSettings)
    yield batch


object StagingProcessor:

  extension (table: Table) def toStagedBatch(namespace: String,
                                             warehouse: String,
                                             batchSchema: ArcaneSchema,
                                             targetName: String,
                                             archiveTableFullName: String,
                                             tablePropertiesSettings: TablePropertiesSettings): StagedVersionedBatch =
    val batchName = table.name().split('.').last
    SynapseLinkMergeBatch(batchName, batchSchema, targetName, archiveTableFullName, tablePropertiesSettings)

  def apply(stagingDataSettings: StagingDataSettings,
            tablePropertiesSettings: TablePropertiesSettings,
            targetTableSettings: TargetTableSettings,
            icebergCatalogSettings: IcebergCatalogSettings,
            archiveTableSettings: ArchiveTableSettings,
            catalogWriter: CatalogWriter[RESTCatalog, Table, Schema]): StagingProcessor =
    new StagingProcessor(stagingDataSettings, tablePropertiesSettings, targetTableSettings, icebergCatalogSettings, archiveTableSettings, catalogWriter)


  type Environment = StagingDataSettings
    & TablePropertiesSettings
    & TargetTableSettings
    & IcebergCatalogSettings
    & ArchiveTableSettings
    & CatalogWriter[RESTCatalog, Table, Schema]


  val layer: ZLayer[Environment, Nothing, StagingProcessor] =
    ZLayer {
      for
        stagingDataSettings <- ZIO.service[StagingDataSettings]
        tablePropertiesSettings <- ZIO.service[TablePropertiesSettings]
        targetTableSettings <- ZIO.service[TargetTableSettings]
        icebergCatalogSettings <- ZIO.service[IcebergCatalogSettings]
        catalogWriter <- ZIO.service[CatalogWriter[RESTCatalog, Table, Schema]]
        archiveTableSettings <- ZIO.service[ArchiveTableSettings]
      yield StagingProcessor(stagingDataSettings, tablePropertiesSettings, targetTableSettings, icebergCatalogSettings, archiveTableSettings, catalogWriter)
    }
