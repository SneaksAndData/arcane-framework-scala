package com.sneaksanddata.arcane.framework
package services.time

import java.time.{LocalDateTime, ZoneOffset}

trait TimeProviderService {
  def time: LocalDateTime
}

object CurrentTimeUTCProvider extends TimeProviderService {
  override def time: LocalDateTime = LocalDateTime.now(ZoneOffset.UTC)
}
