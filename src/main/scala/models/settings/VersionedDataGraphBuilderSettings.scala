package com.sneaksanddata.arcane.framework
package models.settings

import java.time.Duration

/** Provides settings for a stream source.
  */
trait VersionedDataGraphBuilderSettings:

  /** The interval to look back for changes on the startup of the stream.
    */
  @deprecated(
    "This field is deprecated and will be removed in 2.1 release. Use SourceWatermark value for fetching firstVersion instead."
  )
  val lookBackInterval: Duration

  /** The interval for periodic change capture operation.
    */
  val changeCaptureInterval: Duration
