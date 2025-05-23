package com.sneaksanddata.arcane.framework
package services.mssql.base

import zio.Task

import java.time.Duration

/** TODO: DEPRECATED: REPLACE USAGES WITH VersionedDataProvider trait Provides a way to get the changes marked with
  * version from a data source.
  * @tparam DataVersionType
  *   The type of the data version.
  * @tparam DataBatchType
  *   The type of the data batch.
  */
trait MssqlVersionedDataProvider[DataVersionType, DataBatchType] {

  /** Requests the changes from the data source.
    *
    * @param previousVersion
    *   The previous version of the data.
    * @param lookBackInterval
    *   The interval to look back for changes if the version is empty.
    * @return
    *   The changes from the data source.
    */
  def requestChanges(previousVersion: Option[DataVersionType], lookBackInterval: Duration): Task[DataBatchType]

  /** The first version of the data.
    */
  val firstVersion: Option[DataVersionType] = None

}
