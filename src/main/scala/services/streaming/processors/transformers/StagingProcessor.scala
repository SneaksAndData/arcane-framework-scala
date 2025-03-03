package com.sneaksanddata.arcane.framework
package services.streaming.processors.transformers

import logging.ZIOLogAnnotations.zlog
import models.DataCell.schema
import models.settings.{ArchiveTableSettings, StagingDataSettings, TablePropertiesSettings, TargetTableSettings}
import models.{ArcaneSchema, DataRow}
import services.consumers.{ArchiveableBatch, MergeableBatch, StagedVersionedBatch, SynapseLinkMergeBatch}
import services.lakehouse.base.{CatalogWriter, IcebergCatalogSettings}
import services.lakehouse.given_Conversion_ArcaneSchema_Schema
import services.merging.models.{JdbcOptimizationRequest, JdbcOrphanFilesExpirationRequest, JdbcSnapshotExpirationRequest}
import services.streaming.base.{MetadataEnrichedRowStreamElement, RowGroupTransformer}
import services.streaming.processors.transformers.StagingProcessor.toStagedBatch

import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import zio.stream.ZPipeline
import zio.{Chunk, Schedule, Task, ZIO, ZLayer}

import java.time.Duration

trait IndexedStagedBatches(val groupedBySchema: Iterable[StagedVersionedBatch & MergeableBatch & ArchiveableBatch], val batchIndex: Long)


class StagingProcessor(stagingDataSettings: StagingDataSettings,
                       tablePropertiesSettings: TablePropertiesSettings,
                       targetTableSettings: TargetTableSettings,
                       icebergCatalogSettings: IcebergCatalogSettings,
                       archiveTableSettings: ArchiveTableSettings,
                       catalogWriter: CatalogWriter[RESTCatalog, Table, Schema])

  extends RowGroupTransformer:

  private val retryPolicy = Schedule.exponential(Duration.ofSeconds(1)) && Schedule.recurs(10)

  type OutgoingElement = IndexedStagedBatches

  override def process[IncomingElement: MetadataEnrichedRowStreamElement](toInFlightBatch: ToInFlightBatch): ZPipeline[Any, Throwable, Chunk[IncomingElement], OutgoingElement] =
    ZPipeline[Chunk[IncomingElement]]()
      .mapZIO(elements =>
        val groupedBySchema = elements.withFilter(e => e.isDataRow).map(e => e.toDataRow).groupBy(row => row.schema)
        val others = elements.filterNot(e => e.isDataRow)
        val applyTasks = ZIO.foreach(groupedBySchema.keys)(schema => writeDataRows(groupedBySchema(schema), schema))
        applyTasks.map(batches => (batches, others))
      )
      .zipWithIndex
      .map { case ((batches, others), index) => toInFlightBatch(batches, index, others) }

  private def writeDataRows(rows: Chunk[DataRow], arcaneSchema: ArcaneSchema): Task[StagedVersionedBatch & MergeableBatch & ArchiveableBatch] =
    val tableWriterEffect = zlog("Attempting to write data to staging table") *> catalogWriter.write(rows, stagingDataSettings.newStagingTableName, arcaneSchema)
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
                                             tablePropertiesSettings: TablePropertiesSettings): StagedVersionedBatch & MergeableBatch & ArchiveableBatch =
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
