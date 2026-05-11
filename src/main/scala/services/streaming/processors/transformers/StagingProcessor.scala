package com.sneaksanddata.arcane.framework
package services.streaming.processors.transformers

import logging.ZIOLogAnnotations.{getAnnotation, zlog}
import models.app.PluginStreamContext
import models.batches.{MergeableBatch, StagedVersionedBatch}
import models.schemas.{ArcaneSchema, DataRow}
import models.settings.iceberg.IcebergCatalogSettings
import models.settings.staging.StagingTableSettings
import services.iceberg.base.CatalogWriter
import services.iceberg.given_Conversion_ArcaneSchema_Schema
import services.metrics.DeclaredMetrics
import services.metrics.DeclaredMetrics.*
import services.streaming.base.{RowGroupTransformer, StagedBatchProcessor}

import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import zio.stream.ZPipeline
import zio.{Chunk, Task, ZIO, ZLayer}

import scala.collection.parallel.CollectionConverters.*

class StagingProcessor(
    stagingDataSettings: StagingTableSettings,
    targetTableFullName: String,
    icebergCatalogSettings: IcebergCatalogSettings,
    catalogWriter: CatalogWriter[RESTCatalog, Table, Schema],
    declaredMetrics: DeclaredMetrics
) extends RowGroupTransformer:

  type OutgoingElement = StagedBatchProcessor#BatchType

  private def processChunk(
      elements: Chunk[IncomingElement],
      onBatchStaged: OnBatchStaged,
      schema: ArcaneSchema
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

    stagedChunk <- writeDataRows(filteredElements.getOrElse(elements), schema, onBatchStaged, maybeWatermark)
  yield
    if filteredElements.nonEmpty then Chunk(Seq(stagedChunk))
    else
      Chunk(
        Seq(
          onBatchStaged(
            None,
            icebergCatalogSettings.namespace,
            icebergCatalogSettings.warehouse,
            ArcaneSchema.empty(),
            targetTableFullName,
            maybeWatermark
          )
        )
      )

  override def process(
      onStagingTablesComplete: OnStagingTablesComplete,
      onBatchStaged: OnBatchStaged,
      schema: ArcaneSchema
  ): ZPipeline[Any, Throwable, IncomingElement, OutgoingElement] =
    ZPipeline[IncomingElement]
      .mapChunksZIO(elements =>
        for staged <- ZIO.when(elements.nonEmpty)(
            processChunk(elements, onBatchStaged, schema).gaugeDuration(declaredMetrics.batchStageDuration)
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
        targetTableFullName,
        watermarkValue
      )
    yield batch

object StagingProcessor:

  def apply(
      stagingDataSettings: StagingTableSettings,
      targetTableFullName: String,
      icebergCatalogSettings: IcebergCatalogSettings,
      catalogWriter: CatalogWriter[RESTCatalog, Table, Schema],
      declaredMetrics: DeclaredMetrics
  ): StagingProcessor =
    new StagingProcessor(
      stagingDataSettings,
      targetTableFullName,
      icebergCatalogSettings,
      catalogWriter,
      declaredMetrics
    )

  type Environment = PluginStreamContext & CatalogWriter[RESTCatalog, Table, Schema] & DeclaredMetrics

  val layer: ZLayer[Environment, Nothing, StagingProcessor] =
    ZLayer {
      for
        context         <- ZIO.service[PluginStreamContext]
        catalogWriter   <- ZIO.service[CatalogWriter[RESTCatalog, Table, Schema]]
        declaredMetrics <- ZIO.service[DeclaredMetrics]
      yield StagingProcessor(
        context.staging.table,
        context.sink.targetTableFullName,
        context.staging.icebergCatalog,
        catalogWriter,
        declaredMetrics
      )
    }
