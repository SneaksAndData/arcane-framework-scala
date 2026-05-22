package com.sneaksanddata.arcane.framework
package services.backfill.processors

import models.batches.StagedBatch
import models.queries.{DefaultShardCommitQuery, OverwriteQuery, OverwriteReplaceQuery, StreamingBatchQuery}
import models.schemas.{ArcaneSchema, DataRow}
import models.settings.EmptyTablePropertiesSettings
import models.settings.staging.StagingTableSettings
import models.sharding.{BootstrappedShard, SourceShard, StagedShard}
import services.backfill.base.{ShardFactory, ShardStreamProcessor}
import services.iceberg.base.CatalogWriter
import services.iceberg.given_Conversion_ArcaneSchema_Schema
import services.metrics.DeclaredMetrics
import services.streaming.base.JsonWatermark
import services.streaming.batching.StagedBatchFactory

import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import zio.Chunk
import zio.stream.ZPipeline

import java.util.UUID

class ShardStagingProcessor(
    catalogWriter: CatalogWriter[RESTCatalog, Table, Schema],
    shardFactory: ShardFactory,
    declaredMetrics: DeclaredMetrics
) extends ShardStreamProcessor:

  override type OutgoingElement = StagedShard

  override def process(
      shard: BootstrappedShard,
      schema: ArcaneSchema
  ): ZPipeline[Any, Throwable, DataRow, OutgoingElement] = ZPipeline[DataRow]
    .mapChunksZIO { rows =>
      catalogWriter
        .append(rows, shard.shardTableName, schema, Seq())
        .map(_ => Chunk(shardFactory.createStagedShard(shard)))
    }
