package com.sneaksanddata.arcane.framework
package services.streaming.base

import models.batches.StagedBackfillOverwriteBatch
import models.settings.TableName

import zio.Task

/** Creates backfill overwrite batches. The streaming plugins must implement this interface to be able to produce
  * backfill batches needed for the backfill overwrite process.
  */
trait BackfillOverwriteBatchFactory:

  /** Creates a backfill batch.
    * @return
    *   A task that represents the backfill batch.
    */
  def createBackfillBatch(watermark: Option[String]): Task[StagedBackfillOverwriteBatch]
