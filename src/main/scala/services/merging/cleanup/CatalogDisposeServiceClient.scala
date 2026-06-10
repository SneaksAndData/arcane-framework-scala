package com.sneaksanddata.arcane.framework
package services.merging.cleanup

import logging.ZIOLogAnnotations.zlog
import models.batches.StagedBatch
import services.base.{BatchDisposeResult, DisposeServiceClient}
import services.iceberg.base.StagingEntityManager

import zio.{Task, ZIO, ZLayer}

/** Batch dispose client implementation that uses Iceberg REST Catalog API
  */
class CatalogDisposeServiceClient(
    stagingEntityManager: StagingEntityManager
) extends DisposeServiceClient:

  /** Disposes of a batch.
    *
    * @param batch
    *   The batch to dispose.
    * @return
    *   The result of disposing of the batch.
    */
  override def disposeBatch(batch: StagedBatch): Task[BatchDisposeResult] = for
    result <- ZIO.unless(batch.isEmpty)(stagingEntityManager.delete(batch.name).map(BatchDisposeResult(_)))
    _ <- ZIO.when(batch.isEmpty)(zlog("Watermark batch, nothing to dispose") *> ZIO.succeed(BatchDisposeResult(true)))
  yield result.getOrElse(BatchDisposeResult(false))

object CatalogDisposeServiceClient:
  val layer = ZLayer {
    for stagingEntityManager <- ZIO.service[StagingEntityManager]
    yield new CatalogDisposeServiceClient(stagingEntityManager)
  }
