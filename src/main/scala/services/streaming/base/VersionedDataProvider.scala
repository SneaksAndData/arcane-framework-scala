package com.sneaksanddata.arcane.framework
package services.streaming.base

import zio.{Task, ZIO}

/**
 * Provides a way to get the changes marked with version from a data source.
 *
 * @tparam DataVersionType The type of the data version.
 * @tparam DataBatchType   The type of the data batch.
 */
trait VersionedDataProvider[DataVersionType, DataBatchType]:
  def requestChanges(previousVersion: Option[DataVersionType], lookBackInterval: java.time.Duration): Task[DataBatchType]

  /**
   * The first version of the data.
   */
  def firstVersion: Task[DataVersionType]
