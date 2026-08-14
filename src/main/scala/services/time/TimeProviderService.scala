package com.sneaksanddata.arcane.framework
package services.time

import java.time.{Instant, LocalDateTime, ZoneOffset}

trait TimeProviderService {
  def time: LocalDateTime
}

object CurrentTimeUTCProviderService extends TimeProviderService {
  override def time: LocalDateTime =
    LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC)
}
