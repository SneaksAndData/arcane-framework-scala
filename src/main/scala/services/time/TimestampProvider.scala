package com.sneaksanddata.arcane.framework
package services.time

import java.time.{LocalDateTime, ZoneOffset}

trait TimestampProvider:
  def timestamp: LocalDateTime

object CurrentTimestampProvider extends TimestampProvider:
  override def timestamp: LocalDateTime = LocalDateTime.now(ZoneOffset.UTC)
