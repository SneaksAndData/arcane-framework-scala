package com.sneaksanddata.arcane.framework
package services.backfill.processors

import models.schemas.{ArcaneSchema, DataRow}
import models.sharding.{BootstrappedShard, StagedShard}
import services.backfill.base.{ShardFactory, ShardStreamProcessor}
import services.iceberg.base.CatalogWriter
import services.iceberg.given_Conversion_ArcaneSchema_Schema
import services.metrics.DeclaredMetrics

import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import zio.{Chunk, ZIO, ZLayer}
import zio.stream.ZPipeline

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

object ShardStagingProcessor:
  val layer = ZLayer {
    for
      catalogWriter <- ZIO.service[CatalogWriter[RESTCatalog, Table, Schema]]
      shardFactory  <- ZIO.service[ShardFactory]
      metrics       <- ZIO.service[DeclaredMetrics]
    yield new ShardStagingProcessor(
      catalogWriter = catalogWriter,
      shardFactory = shardFactory,
      declaredMetrics = metrics
    )
  }
