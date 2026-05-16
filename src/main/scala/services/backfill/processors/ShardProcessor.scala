package com.sneaksanddata.arcane.framework
package services.backfill.processors

import services.streaming.base.{StreamingBatchProcessor, StreamingRowGroupTransformer}

import com.sneaksanddata.arcane.framework.models.batches.StagedBatch
import com.sneaksanddata.arcane.framework.models.queries.StreamingBatchQuery
import com.sneaksanddata.arcane.framework.models.schemas.ArcaneSchema
import com.sneaksanddata.arcane.framework.models.settings.iceberg.IcebergCatalogSettings
import com.sneaksanddata.arcane.framework.models.settings.staging.StagingTableSettings
import com.sneaksanddata.arcane.framework.models.sharding.SourceShard
import com.sneaksanddata.arcane.framework.services.backfill.BackfillRowGroupTransformer
import com.sneaksanddata.arcane.framework.services.iceberg.base.CatalogWriter
import com.sneaksanddata.arcane.framework.services.metrics.DeclaredMetrics
import com.sneaksanddata.arcane.framework.services.streaming.batching.StagedBatchFactory
import com.sneaksanddata.arcane.framework.services.iceberg.given_Conversion_ArcaneSchema_Schema
import com.sneaksanddata.arcane.framework.models.sharding.SourceShardExtensions.*
import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import zio.Chunk
import zio.stream.{ZPipeline, ZSink}

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
                       catalogWriter: CatalogWriter[RESTCatalog, Table, Schema],
                       batchFactory: StagedBatchFactory,
                       declaredMetrics: DeclaredMetrics
                     ) extends BackfillRowGroupTransformer:
  
  override type OutgoingElement = StagedShardBatch
  
  override def process(shard: SourceShard, schema: ArcaneSchema): ZPipeline[Any, Throwable, IncomingElement, OutgoingElement] = ZPipeline[IncomingElement]
    // TODO: bit of a mess with table name, refactor
    .mapChunksZIO(rows => catalogWriter.append(rows, shard.getStagingTableName(stagingDataSettings.stagingTablePrefix), schema, Seq()).map(_ => Chunk(1)))
    .map(_ => StagedShardBatch(targetTableFullName, shard.getStagingTableName(stagingDataSettings.stagingTablePrefix), schema))
    
    
