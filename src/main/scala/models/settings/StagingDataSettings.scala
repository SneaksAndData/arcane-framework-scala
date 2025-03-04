package com.sneaksanddata.arcane.framework
package models.settings

import java.time.{ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.UUID

trait StagingDataSettings:
  val stagingTablePrefix: String
  val retryPolicy: RetrySettings
  
  def newStagingTableName: String =
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss")
    val id = UUID.randomUUID().toString
    s"${stagingTablePrefix}__${ZonedDateTime.now(ZoneOffset.UTC).format(formatter)}_$id".replace('-', '_')
