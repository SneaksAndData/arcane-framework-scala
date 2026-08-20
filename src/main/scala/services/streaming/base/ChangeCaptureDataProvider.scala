package com.sneaksanddata.arcane.framework
package services.streaming.base

import models.schemas.DataRow

import zio.Task
import zio.stream.ZStream

import java.time.OffsetDateTime

/** Provides a way to get the changes marked with version from a data source.
  *
  * @tparam DataVersionType
  *   The type of the data version.
  */
trait ChangeCaptureDataProvider[DataVersionType <: SourceWatermark[String]]:
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
  ): ZStream[Any, Throwable, StructuredZStream]

  /** The first version of the data
    */
  def currentWatermark: Task[DataVersionType]

  /** Find latest watermark in between provided ones, taking that no more than `maxRangeSize` watermarks can be taken
    * into evaluation For example, if diff(startWatermark, endWatermark) contains 10 elements [wm0, wm1, wm2, ...], but
    * maxRangeSize is set to 3, the returned watermark will be `wm2`
    */
  def getLatestWatermarkInRange(
      startWatermark: DataVersionType,
      endWatermark: DataVersionType,
      maxRangeSize: Int
  ): Task[DataVersionType]
