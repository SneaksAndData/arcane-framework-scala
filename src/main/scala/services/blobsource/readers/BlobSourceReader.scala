package com.sneaksanddata.arcane.framework
package services.blobsource.readers

import models.schemas.DataRow
import services.blobsource.versioning.BlobSourceWatermark

import zio.Task
import zio.stream.ZStream

import java.time.Duration

/** Base trait for all blob source readers
  */
trait BlobSourceReader:
  /** Output row type for this reader. Typically DataRow or GenericRecord
    */
  type OutputRow = DataRow

  /** Change stream for this reader. If startFrom == 0, should behave like a backfill.
    * @param startFrom
    *   Time (Unix) to emit changes from for next iteration
    * @return
    */
  def getChanges(startFrom: BlobSourceWatermark): ZStream[Any, Throwable, OutputRow]

  def getLatestVersion: Task[BlobSourceWatermark]

  def hasChanges(previousVersion: BlobSourceWatermark): Task[Boolean]
