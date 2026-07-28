package com.sneaksanddata.arcane.framework
package services.completion.base

import zio.Task

/** Defines additional tasks to execute on stream completion.
  */
trait StreamFinalizer:
  /** Backfill finalizer
    */
  def finalizeBackfill: Task[Unit]

  /** Change capture stream finalizer
    * @return
    */
  def finalizeChangeCapture: Task[Unit]
