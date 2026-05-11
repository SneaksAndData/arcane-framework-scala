package com.sneaksanddata.arcane.framework
package services.merging.cleanup

import models.batches.{MergeableBatch, StagedBatch, StagedVersionedBatch}
import services.base.{BatchDisposeResult, DisposeServiceClient}
import services.iceberg.base.StagingEntityManager

import zio.Task

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
  override def disposeBatch(batch: StagedBatch): Task[BatchDisposeResult] =
    stagingEntityManager.delete(batch.name).map(BatchDisposeResult(_))
