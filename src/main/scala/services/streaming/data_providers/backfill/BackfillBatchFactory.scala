package com.sneaksanddata.arcane.framework
package services.streaming.data_providers.backfill

import services.consumers.StagedBackfillOverwriteBatch

import zio.Task

/**
 * Creates backfill overwrite batches.
 */
trait BackfillBatchFactory:
  
  /**
   * Creates a backfill batch.
   *
   * @param intermediateTableName The name of the intermediate table.
   * @return A task that represents the backfill batch.
   */
  def createBackfillBatch: Task[StagedBackfillOverwriteBatch]

//  private def createBackfillBatch(tableName: String): Task[StagedBackfillOverwriteBatch] =
//    for schema <- jdbcMergeServiceClient.getSchema(tableName)
//      yield SynapseLinkBackfillOverwriteBatch(tableName,
//        schema,
//        sinkSettings.targetTableFullName,
//        tablePropertiesSettings)
//
//
