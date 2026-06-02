package com.sneaksanddata.arcane.framework
package services.backfill.processors

import logging.ZIOLogAnnotations.getAnnotation
import models.schemas.{ArcaneSchema, DataRow}
import models.sharding.{BootstrappedShard, StagedShard}
import services.backfill.base.{ShardFactory, ShardStreamProcessor}
import services.iceberg.base.CatalogWriter
import services.iceberg.given_Conversion_ArcaneSchema_Schema
import services.metrics.DeclaredMetrics
import services.naming.NameGenerator

import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import zio.stream.ZPipeline
import zio.{Chunk, ZIO, ZLayer}

class ShardStagingProcessor(
    catalogWriter: CatalogWriter[RESTCatalog, Table, Schema],
    shardFactory: ShardFactory,
    nameGenerator: NameGenerator,
    declaredMetrics: DeclaredMetrics
) extends ShardStreamProcessor:

  override type OutgoingElement = StagedShard

  override def process(
      shard: BootstrappedShard,
      schema: ArcaneSchema
  ): ZPipeline[Any, Throwable, DataRow, OutgoingElement] = ZPipeline[DataRow]
    .mapChunksZIO { rows =>
      for
        shardTableName <- nameGenerator.getShardTableName(shard)
        stagedShard <- catalogWriter
          .append(rows, shardTableName, schema, Seq(getAnnotation("processor", "ShardStagingProcessor")))
          .flatMap(_ => shardFactory.createStagedShard(shard).map(v => Chunk(v)))
      yield stagedShard
    }

object ShardStagingProcessor:
  val layer = ZLayer {
    for
      catalogWriter <- ZIO.service[CatalogWriter[RESTCatalog, Table, Schema]]
      shardFactory  <- ZIO.service[ShardFactory]
      metrics       <- ZIO.service[DeclaredMetrics]
      nameGenerator <- ZIO.service[NameGenerator]
    yield new ShardStagingProcessor(
      catalogWriter = catalogWriter,
      shardFactory = shardFactory,
      declaredMetrics = metrics,
      nameGenerator = nameGenerator
    )
  }
