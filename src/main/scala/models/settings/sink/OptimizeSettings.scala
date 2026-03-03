package com.sneaksanddata.arcane.framework
package models.settings.sink

/** Settings for optimizing the data table
  */
trait OptimizeSettings:

  /** Number of batches to trigger optimization
    */
  val batchThreshold: Int

  /** Optimize when the file size exceeds this threshold
    */
  val fileSizeThreshold: String
