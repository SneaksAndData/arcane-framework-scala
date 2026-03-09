package com.sneaksanddata.arcane.framework
package models.settings.staging

import models.settings.iceberg.{DefaultIcebergStagingSettings, IcebergStagingSettings}

import upickle.ReadWriter

/** Staging configuration
  */
trait StagingSettings:
  /** Settings for staging tables management
    */
  val table: StagingTableSettings

  /** Merge client configuration
    */
  val mergeServiceClient: JdbcMergeServiceClientSettings

  /** Iceberg REST Catalog configuration for staging tables
    */
  val icebergCatalog: IcebergStagingSettings

case class DefaultStagingSettings(
    override val table: DefaultStagingTableSettings,
    override val icebergCatalog: DefaultIcebergStagingSettings,
    override val mergeServiceClient: DefaultJdbcMergeServiceClientSettings
) extends StagingSettings derives ReadWriter
