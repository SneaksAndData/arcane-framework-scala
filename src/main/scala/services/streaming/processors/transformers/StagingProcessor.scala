package com.sneaksanddata.arcane.framework
package services.streaming.processors.transformers

import logging.ZIOLogAnnotations.{getAnnotation, zlog}
import models.batches.{MergeableBatch, StagedVersionedBatch}
import models.schemas.DataCell.schema
import models.schemas.{ArcaneSchema, DataRow}
import models.settings.{IcebergStagingSettings, SinkSettings, StagingDataSettings, TablePropertiesSettings}
import services.iceberg.base.CatalogWriter
import services.iceberg.given_Conversion_ArcaneSchema_Schema
import services.metrics.DeclaredMetrics
import services.metrics.DeclaredMetrics.*
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
    targetTableSettings: SinkSettings,
    icebergCatalogSettings: IcebergStagingSettings,
    catalogWriter: CatalogWriter[RESTCatalog, Table, Schema],
    declaredMetrics: DeclaredMetrics
) extends RowGroupTransformer:

  type OutgoingElement = StagedBatchProcessor#BatchType

  private def processChunk(
      elements: Chunk[IncomingElement],
      onBatchStaged: OnBatchStaged
  ): ZIO[Any, Throwable, Chunk[Iterable[StagedVersionedBatch & MergeableBatch]]] = for
    _ <- zlog(
      "Started preparing a batch of size %s for staging",
      Seq(getAnnotation("processor", "StagingProcessor")),
      elements.size.toString
    )
    // check if watermark row has been emitted and pass it down the pipeline
    maybeWatermark <- ZIO.attempt(elements.par.find(_.isWatermark).flatMap(_.getWatermark))
    // avoid failure by trying to commit watermark row if present
    filteredElements <- ZIO.when(maybeWatermark.isDefined) {
      for filtered <- ZIO.filterPar(elements)(r => ZIO.succeed(!r.isWatermark))
      yield filtered
    }
    _ <- ZIO.succeed(filteredElements.getOrElse(elements).size.toLong) @@ declaredMetrics.rowsIncoming
    groupedBySchema <-
      (if stagingDataSettings.isUnifiedSchema then
         ZIO.succeed(
           Map(
             filteredElements
               .getOrElse(elements)
               .headOption
               .map(_.schema)
               .getOrElse(ArcaneSchema.empty()) -> filteredElements.getOrElse(elements)
           )
         )
       else
         ZIO.succeed(
           filteredElements
             .getOrElse(elements)
             .toArray
             .par
             .map(r => r.schema -> r)
             .aggregate(Map.empty[ArcaneSchema, Chunk[IncomingElement]])(
               (agg, element) => mergeGroupedChunks(agg, element.toChunkMap),
               mergeGroupedChunks
             )
         )
      ).gaugeDuration(declaredMetrics.batchTransformDuration)

    _ <- ZIO.when((maybeWatermark.isDefined && filteredElements.exists(_.nonEmpty)) || maybeWatermark.isEmpty)(
      zlog(
        "Batch of size %s is ready for staging",
        Seq(getAnnotation("processor", "StagingProcessor")),
        filteredElements.getOrElse(elements).size.toString
      )
    )
    _ <- ZIO.when(maybeWatermark.isDefined && !filteredElements.exists(_.nonEmpty))(
      zlog(
        "Batch contains watermark only. Staging and merge operations will be skipped, but maintenance may still occur",
        Seq(getAnnotation("processor", "StagingProcessor"))
      )
    )

    applyTasks <- ZIO.foreach(groupedBySchema.keys)(schema =>
      writeDataRows(groupedBySchema(schema), schema, onBatchStaged, maybeWatermark)
    )
  yield Chunk(applyTasks.map(batches => batches))

  override def process(
      onStagingTablesComplete: OnStagingTablesComplete,
      onBatchStaged: OnBatchStaged
  ): ZPipeline[Any, Throwable, IncomingElement, OutgoingElement] =
    ZPipeline[IncomingElement]()
      .mapChunksZIO(elements =>
        for staged <- ZIO.when(elements.nonEmpty)(
            processChunk(elements, onBatchStaged).gaugeDuration(declaredMetrics.batchStageDuration)
          )
        yield staged.getOrElse(Chunk.empty)
      )
      .filter(_.nonEmpty)
      .zipWithIndex
      .map { case (batches, index) => onStagingTablesComplete(batches, index, Chunk()) }

  private def writeDataRows(
      rows: Chunk[DataRow],
      arcaneSchema: ArcaneSchema,
      onBatchStaged: OnBatchStaged,
      watermarkValue: Option[String]
  ): Task[StagedVersionedBatch & MergeableBatch] =
    for
      table <- ZIO.when(rows.nonEmpty)(
        catalogWriter.write(
          rows,
          stagingDataSettings.newStagingTableName,
          arcaneSchema,
          Seq(getAnnotation("processor", "StagingProcessor"))
        )
      )
      batch = onBatchStaged(
        table,
        icebergCatalogSettings.namespace,
        icebergCatalogSettings.warehouse,
        arcaneSchema,
        targetTableSettings.targetTableFullName,
        tablePropertiesSettings,
        watermarkValue
      )
    yield batch

object StagingProcessor:

  def apply(
      stagingDataSettings: StagingDataSettings,
      tablePropertiesSettings: TablePropertiesSettings,
      targetTableSettings: SinkSettings,
      icebergCatalogSettings: IcebergStagingSettings,
      catalogWriter: CatalogWriter[RESTCatalog, Table, Schema],
      declaredMetrics: DeclaredMetrics
  ): StagingProcessor =
    new StagingProcessor(
      stagingDataSettings,
      tablePropertiesSettings,
      targetTableSettings,
      icebergCatalogSettings,
      catalogWriter,
      declaredMetrics
    )

  type Environment = StagingDataSettings & TablePropertiesSettings & SinkSettings & IcebergStagingSettings &
    CatalogWriter[RESTCatalog, Table, Schema] & DeclaredMetrics

  val layer: ZLayer[Environment, Nothing, StagingProcessor] =
    ZLayer {
      for
        stagingDataSettings     <- ZIO.service[StagingDataSettings]
        tablePropertiesSettings <- ZIO.service[TablePropertiesSettings]
        targetTableSettings     <- ZIO.service[SinkSettings]
        icebergCatalogSettings  <- ZIO.service[IcebergStagingSettings]
        catalogWriter           <- ZIO.service[CatalogWriter[RESTCatalog, Table, Schema]]
        declaredMetrics         <- ZIO.service[DeclaredMetrics]
      yield StagingProcessor(
        stagingDataSettings,
        tablePropertiesSettings,
        targetTableSettings,
        icebergCatalogSettings,
        catalogWriter,
        declaredMetrics
      )
    }
