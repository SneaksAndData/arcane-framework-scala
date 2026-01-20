package com.sneaksanddata.arcane.framework
package services.streaming.base

import java.time.OffsetDateTime

trait SourceWatermark[VersionType <: String] extends Ordered[SourceWatermark[VersionType]]:
  val version: VersionType
  val timestamp: OffsetDateTime
