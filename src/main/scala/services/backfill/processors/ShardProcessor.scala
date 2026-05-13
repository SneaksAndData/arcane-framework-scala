package com.sneaksanddata.arcane.framework
package services.backfill.processors

import services.streaming.base.{RowGroupTransformer, StreamingBatchProcessor}

import com.sneaksanddata.arcane.framework.models.batches.StagedBatch
import com.sneaksanddata.arcane.framework.models.queries.StreamingBatchQuery
import com.sneaksanddata.arcane.framework.models.schemas.ArcaneSchema
import com.sneaksanddata.arcane.framework.models.settings.iceberg.IcebergCatalogSettings
import com.sneaksanddata.arcane.framework.models.settings.staging.StagingTableSettings
import com.sneaksanddata.arcane.framework.services.iceberg.base.CatalogWriter
import com.sneaksanddata.arcane.framework.services.metrics.DeclaredMetrics
import com.sneaksanddata.arcane.framework.services.streaming.batching.StagedBatchFactory
import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import zio.stream.ZPipeline

class ShardCompletionQuery(targetName: String, sourceName: String) extends StreamingBatchQuery:
  // TODO: apply reduce expr
  override def query: String = s"INSERT INTO $targetName SELECT * FROM $sourceName"

class StagedShardBatch(targetName: String, tableName: String, batchSchema: ArcaneSchema) extends StagedBatch:
  override type Query = ShardCompletionQuery
  
  override val schema: ArcaneSchema = batchSchema
  override val batchQuery: Query = ShardCompletionQuery(targetName, tableName)
  override val name: String = tableName
  override val completedWatermarkValue: Option[String] = None

  override def reduceExpr: String = ???
  
class WatermarkShardBatch(watermark: String) extends StagedBatch:
  override val name: String = "watermark"
  override val completedWatermarkValue: Option[String] = Some(watermark)

  
  
class ShardProcessor (
                       stagingDataSettings: StagingTableSettings,
                       targetTableFullName: String,
                       icebergCatalogSettings: IcebergCatalogSettings,
                       catalogWriter: CatalogWriter[RESTCatalog, Table, Schema],
                       batchFactory: StagedBatchFactory,
                       declaredMetrics: DeclaredMetrics
                     ) extends RowGroupTransformer:
  
  override type OutgoingElement = StagedShardBatch
  
  override def process(schema: ArcaneSchema): ZPipeline[Any, Throwable, IncomingElement, OutgoingElement] = ???
