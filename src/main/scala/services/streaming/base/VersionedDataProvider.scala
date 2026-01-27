package com.sneaksanddata.arcane.framework
package services.streaming.base

import models.schemas.DataRow

import zio.Task
import zio.stream.ZStream

/** Provides a way to get the changes marked with version from a data source.
  *
  * @tparam DataVersionType
  *   The type of the data version.
  * @tparam DataBatchType
  *   The type of the data batch.
  */
trait VersionedDataProvider[DataVersionType <: SourceWatermark[String], DataBatchType <: DataRow]:
  /** Checks whether the provided watermark from previous iteration has accrued any changes in [previousVersion ... now]
    * interval
    * @param previousVersion
    *   Watermark from the previous change capture iteration
    * @return
    */
  def hasChanges(previousVersion: DataVersionType): Task[Boolean]

  /** Most recent version of a source dataset, compared. This should return previousVersion in case retrieval of a most
    * recent version failed.
    * @param previousVersion
    *   Watermark from the previous change capture iteration
    * @return
    */
  def getCurrentVersion(previousVersion: DataVersionType): Task[DataVersionType]

  /** Request a next set of changes from source, that fall into interval from `previousVersion` to `now`
    * @param previousVersion
    *   Watermark from the previous change capture iteration
    * @return
    */
  def requestChanges(
      previousVersion: DataVersionType,
      nextVersion: DataVersionType
  ): ZStream[Any, Throwable, DataBatchType]

  /** The first version of the data.
    */
  def firstVersion: Task[DataVersionType]
