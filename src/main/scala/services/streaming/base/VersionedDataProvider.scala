package com.sneaksanddata.arcane.framework
package services.streaming.base

import zio.Task
import zio.stream.ZStream

/** Provides a way to get the changes marked with version from a data source.
  *
  * @tparam DataVersionType
  *   The type of the data version.
  * @tparam DataBatchType
  *   The type of the data batch.
  */
trait VersionedDataProvider[DataVersionType, DataBatchType]:
  def requestChanges(previousVersion: DataVersionType): ZStream[Any, Throwable, DataBatchType]

  /** The first version of the data.
    */
  def firstVersion: Task[DataVersionType]
