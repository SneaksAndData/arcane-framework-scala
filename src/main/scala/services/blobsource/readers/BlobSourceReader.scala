package com.sneaksanddata.arcane.framework
package services.blobsource.readers

import services.blobsource.BlobSourceVersion

import zio.Task
import zio.stream.ZStream

import java.time.Duration

/** Base trait for all blob source readers
  */
trait BlobSourceReader:
  /** Output row type for this reader. Typically DataRow or GenericRecord
    */
  type OutputRow

  /** Change stream for this reader. If startFrom == 0, should behave like a backfill.
    * @param startFrom
    *   Time (Unix) to emit changes from for next iteration
    * @return
    */
  def getChanges(startFrom: BlobSourceVersion): ZStream[Any, Throwable, OutputRow]

  def getStartFrom(lookBackInterval: Duration): Task[BlobSourceVersion]

  def getLatestVersion: Task[BlobSourceVersion]
