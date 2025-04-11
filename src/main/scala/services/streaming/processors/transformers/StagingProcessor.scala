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

import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import zio.stream.ZPipeline
import zio.{Chunk, Schedule, Task, ZIO, ZLayer}
import scala.collection.parallel.CollectionConverters._

import java.time.Duration

trait IndexedStagedBatches(val groupedBySchema: Iterable[StagedVersionedBatch & MergeableBatch], val batchIndex: Long)


class StagingProcessor(stagingDataSettings: StagingDataSettings,
                       tablePropertiesSettings: TablePropertiesSettings,
                       targetTableSettings: TargetTableSettings,
                       icebergCatalogSettings: IcebergCatalogSettings,
                       catalogWriter: CatalogWriter[RESTCatalog, Table, Schema])

  extends RowGroupTransformer:

  type OutgoingElement = StagedBatchProcessor#BatchType
  
  override def process(onStagingTablesComplete: OnStagingTablesComplete, onBatchStaged: OnBatchStaged): ZPipeline[Any, Throwable, Chunk[IncomingElement], OutgoingElement] =
    ZPipeline[Chunk[IncomingElement]]()
      .filter(_.nonEmpty)
      .mapZIO(elements => for
         _ <- zlog("Started preparing a batch of size %s for staging", elements.size.toString)  
       yield elements)
      .mapZIO(elements =>
        val groupedBySchema = elements
          .par
          .map(r => r.schema -> r)
          .aggregate(Map.empty[ArcaneSchema, Chunk[IncomingElement]])(
            (agg, element) => (agg.toSeq ++ Map(element._1 -> Chunk(element._2))).groupMap(_._1)(_._2).map {
              case (key, chunks) => key -> Chunk.from(chunks.flatten)
            }, 
            (a, b) => (a.toSeq ++ b).groupMap(_._1)(_._2).map { 
              case (key, chunks) => key -> Chunk.from(chunks.flatten)
            })
        val applyTasks = ZIO.foreach(groupedBySchema.keys)(schema => writeDataRows(groupedBySchema(schema), schema, onBatchStaged))
        applyTasks.map(batches => batches)
      )
      .mapZIO(elements => for
        _ <- zlog("Finished preparing a batch for staging")
      yield elements)
      .zipWithIndex
      .map { case (batches, index) => onStagingTablesComplete(batches, index, Chunk()) }

  private def writeDataRows(rows: Chunk[DataRow], arcaneSchema: ArcaneSchema, onBatchStaged: OnBatchStaged): Task[StagedVersionedBatch & MergeableBatch] =
    for
      table <- catalogWriter.write(rows, stagingDataSettings.newStagingTableName, arcaneSchema)
      batch = onBatchStaged(table,
        icebergCatalogSettings.namespace,
        icebergCatalogSettings.warehouse,
        arcaneSchema,
        targetTableSettings.targetTableFullName,
        tablePropertiesSettings)
    yield batch


object StagingProcessor:

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
