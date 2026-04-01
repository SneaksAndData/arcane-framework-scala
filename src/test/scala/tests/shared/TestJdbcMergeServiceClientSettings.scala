package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.staging.{JdbcMergeServiceClientSettings, JdbcQueryRetryMode, Never, NeverImpl}

object TestJdbcMergeServiceClientSettings extends JdbcMergeServiceClientSettings:
  /** The connection URL.
    */
  override val connectionUrl: String = "jdbc:trino://localhost:8080/iceberg/test?user=test"

  override val extraConnectionParameters: Map[String, String] = Map()

  override val queryRetryMode: JdbcQueryRetryMode        = NeverImpl(Never())
  override val queryRetryOnMessageContents: List[String] = List()
  override val queryRetryBaseDuration: zio.Duration      = zio.Duration.Zero
  override val queryRetryMaxAttempts: Int                = 1
  override val queryRetryScaleFactor: Double             = 1
