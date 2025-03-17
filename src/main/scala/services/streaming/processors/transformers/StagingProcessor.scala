package com.sneaksanddata.arcane.framework
package services.streaming.processors.transformers

import logging.ZIOLogAnnotations.zlog
import models.DataCell.schema
import models.settings.{StagingDataSettings, TablePropertiesSettings, TargetTableSettings}
import models.{ArcaneSchema, BatchIdCell, BatchIdField, DataCell, DataRow}
import services.consumers.{MergeableBatch, StagedVersionedBatch, SynapseLinkMergeBatch}
import services.lakehouse.base.{CatalogWriter, IcebergCatalogSettings}
import services.lakehouse.given_Conversion_ArcaneSchema_Schema
import services.streaming.base.{MetadataEnrichedRowStreamElement, RowGroupTransformer, StagedBatchProcessor}

import com.sneaksanddata.arcane.framework.models.ArcaneType.{LongType, StringType}
import com.sneaksanddata.arcane.framework.services.merging.JdbcTableManager
import com.sneaksanddata.arcane.framework.services.streaming.processors.transformers.StagingProcessor.addBatchId
import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import zio.stream.ZPipeline
import zio.{Chunk, Schedule, Task, ZIO, ZLayer}

import java.time.Duration
import java.util.UUID

trait IndexedStagedBatches(val groupedBySchema: Iterable[StagedVersionedBatch & MergeableBatch], val batchIndex: Long)


class StagingProcessor(stagingDataSettings: StagingDataSettings,
                       tablePropertiesSettings: TablePropertiesSettings,
                       targetTableSettings: TargetTableSettings,
                       icebergCatalogSettings: IcebergCatalogSettings,
                       catalogWriter: CatalogWriter[RESTCatalog, Table, Schema],
                       tableManager: JdbcTableManager)

  extends RowGroupTransformer:

  private val retryPolicy = Schedule.exponential(Duration.ofSeconds(1)) && Schedule.recurs(10)

  type OutgoingElement = StagedBatchProcessor#BatchType
  
  type IncomingElement = DataRow|Any

  override def process(onStagingTablesComplete: OnStagingTablesComplete, onBatchStaged: OnBatchStaged): ZPipeline[Any, Throwable, Chunk[IncomingElement], OutgoingElement] =
    ZPipeline[Chunk[IncomingElement]]()
      .filter(_.nonEmpty)
      .mapZIO(elements =>
        val groupedBySchema = elements.withFilter(e => e.isInstanceOf[DataRow]).map(e => e.asInstanceOf[DataRow]).groupBy(row => row.schema)
        val others = elements.filterNot(e => e.isInstanceOf[DataRow])
        val applyTasks = ZIO.foreach(groupedBySchema.keys) { schema =>
          val batchId = UUID.randomUUID().toString
          writeDataRows(groupedBySchema(schema).map(r => r.addBatchId(batchId)), batchId, schema.addBatchId(), onBatchStaged)
        }
        applyTasks.map(batches => (batches, others))
      )
      .zipWithIndex
      .map { case ((batches, others), index) => onStagingTablesComplete(batches, index, others) }

  private def writeDataRows(rows: Chunk[DataRow], batchId: String, arcaneSchema: ArcaneSchema, onBatchStaged: OnBatchStaged): Task[StagedVersionedBatch & MergeableBatch] =

    for
      _ <- tableManager.migrateSchema(arcaneSchema, stagingDataSettings.getStagingTableName)
      newSchema <- tableManager.getSchema(stagingDataSettings.getStagingTableName)
      ordering = newSchema.map(_.name).zipWithIndex.toMap

      table <- catalogWriter.append(rows.map(r => r.sortBy(c => ordering(c.name))), stagingDataSettings.getStagingTableName, arcaneSchema)
        .tapErrorCause(cause => zlog("Error writing data to staging table: {cause}", cause))
        .retry(retryPolicy)

      batch = onBatchStaged(table,
        batchId,
        icebergCatalogSettings.namespace,
        icebergCatalogSettings.warehouse,
        arcaneSchema,
        targetTableSettings.targetTableFullName,
        tablePropertiesSettings)
    yield batch

object StagingProcessor:

  /**
   * Adds a batch id to the row.
   *
   * @param batchId The batch id to add.
   * @param row The row to add the batch id to.
   * @return The row with the batch id added.
   */
  extension (row: DataRow) def addBatchId(batchId: String): DataRow = row :+ BatchIdCell(batchId)

  /**
   * Adds a batch id to the schema.
   *
   * @param schema The schema to add the batch id to.
   * @return The schema with the batch id added.
   */
  extension (schema: ArcaneSchema) def addBatchId(): ArcaneSchema = schema :+ BatchIdField

  type Environment = StagingDataSettings
    & TablePropertiesSettings
    & TargetTableSettings
    & IcebergCatalogSettings
    & CatalogWriter[RESTCatalog, Table, Schema]
    & JdbcTableManager

  def apply(stagingDataSettings: StagingDataSettings,
            tablePropertiesSettings: TablePropertiesSettings,
            targetTableSettings: TargetTableSettings,
            icebergCatalogSettings: IcebergCatalogSettings,
            catalogWriter: CatalogWriter[RESTCatalog, Table, Schema],
            tableManager: JdbcTableManager): StagingProcessor =
    new StagingProcessor(stagingDataSettings, tablePropertiesSettings, targetTableSettings, icebergCatalogSettings, catalogWriter, tableManager)

  val layer: ZLayer[Environment, Nothing, StagingProcessor] =
    ZLayer {
      for
        stagingDataSettings <- ZIO.service[StagingDataSettings]
        tablePropertiesSettings <- ZIO.service[TablePropertiesSettings]
        targetTableSettings <- ZIO.service[TargetTableSettings]
        icebergCatalogSettings <- ZIO.service[IcebergCatalogSettings]
        catalogWriter <- ZIO.service[CatalogWriter[RESTCatalog, Table, Schema]]
        tableManager <- ZIO.service[JdbcTableManager]
      yield StagingProcessor(stagingDataSettings, tablePropertiesSettings, targetTableSettings, icebergCatalogSettings, catalogWriter, tableManager)
    }
