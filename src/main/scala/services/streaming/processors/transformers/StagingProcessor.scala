package com.sneaksanddata.arcane.framework
package services.streaming.processors.transformers

import logging.ZIOLogAnnotations.zlog
import models.batches.{MergeableBatch, StagedVersionedBatch}
import models.schemas.DataCell.schema
import models.schemas.{ArcaneSchema, DataRow}
import models.settings.{IcebergCatalogSettings, StagingDataSettings, TablePropertiesSettings, TargetTableSettings}
import services.iceberg.base.CatalogWriter
import services.iceberg.given_Conversion_ArcaneSchema_Schema
import services.streaming.base.{RowGroupTransformer, StagedBatchProcessor}
import utils.CollectionUtils.*

import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import zio.stream.ZPipeline
import zio.{Chunk, Task, ZIO, ZLayer}

import scala.collection.parallel.CollectionConverters.*

trait IndexedStagedBatches(val groupedBySchema: Iterable[StagedVersionedBatch & MergeableBatch], val batchIndex: Long)

class StagingProcessor(
    stagingDataSettings: StagingDataSettings,
    tablePropertiesSettings: TablePropertiesSettings,
    targetTableSettings: TargetTableSettings,
    icebergCatalogSettings: IcebergCatalogSettings,
    catalogWriter: CatalogWriter[RESTCatalog, Table, Schema]
) extends RowGroupTransformer:

  type OutgoingElement = StagedBatchProcessor#BatchType

  override def process(
      onStagingTablesComplete: OnStagingTablesComplete,
      onBatchStaged: OnBatchStaged
  ): ZPipeline[Any, Throwable, Chunk[IncomingElement], OutgoingElement] = {
    ZPipeline[Chunk[IncomingElement]]()
      .filter(_.nonEmpty)
      .mapZIO(elements =>
        for
          _ <- zlog("Started preparing a batch of size %s for staging", elements.size.toString)
          groupedBySchema <-
            if stagingDataSettings.isUnifiedSchema then ZIO.succeed(Map(elements.head.schema -> elements))
            else
              ZIO.succeed(
                elements.toArray.par
                  .map(r => r.schema -> r)
                  .aggregate(Map.empty[ArcaneSchema, Chunk[IncomingElement]])(
                    (agg, element) => mergeGroupedChunks(agg, element.toChunkMap),
                    mergeGroupedChunks
                  )
              )
          _ <- zlog("Batch is ready for staging")
          applyTasks <- ZIO.foreach(groupedBySchema.keys)(schema =>
            writeDataRows(groupedBySchema(schema), schema, onBatchStaged)
          )
        yield applyTasks.map(batches => batches)
      )
      .zipWithIndex
      .map { case (batches, index) => onStagingTablesComplete(batches, index, Chunk()) }
  }

  private def writeDataRows(
      rows: Chunk[DataRow],
      arcaneSchema: ArcaneSchema,
      onBatchStaged: OnBatchStaged
  ): Task[StagedVersionedBatch & MergeableBatch] =
    for
      table <- catalogWriter.write(rows, stagingDataSettings.newStagingTableName, arcaneSchema)
      batch = onBatchStaged(
        table,
        icebergCatalogSettings.namespace,
        icebergCatalogSettings.warehouse,
        arcaneSchema,
        targetTableSettings.targetTableFullName,
        tablePropertiesSettings
      )
    yield batch

object StagingProcessor:

  def apply(
      stagingDataSettings: StagingDataSettings,
      tablePropertiesSettings: TablePropertiesSettings,
      targetTableSettings: TargetTableSettings,
      icebergCatalogSettings: IcebergCatalogSettings,
      catalogWriter: CatalogWriter[RESTCatalog, Table, Schema]
  ): StagingProcessor =
    new StagingProcessor(
      stagingDataSettings,
      tablePropertiesSettings,
      targetTableSettings,
      icebergCatalogSettings,
      catalogWriter
    )

  type Environment = StagingDataSettings & TablePropertiesSettings & TargetTableSettings & IcebergCatalogSettings &
    CatalogWriter[RESTCatalog, Table, Schema]

  val layer: ZLayer[Environment, Nothing, StagingProcessor] =
    ZLayer {
      for
        stagingDataSettings     <- ZIO.service[StagingDataSettings]
        tablePropertiesSettings <- ZIO.service[TablePropertiesSettings]
        targetTableSettings     <- ZIO.service[TargetTableSettings]
        icebergCatalogSettings  <- ZIO.service[IcebergCatalogSettings]
        catalogWriter           <- ZIO.service[CatalogWriter[RESTCatalog, Table, Schema]]
      yield StagingProcessor(
        stagingDataSettings,
        tablePropertiesSettings,
        targetTableSettings,
        icebergCatalogSettings,
        catalogWriter
      )
    }
