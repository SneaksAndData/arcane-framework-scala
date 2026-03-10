package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.iceberg.IcebergStagingSettings
import models.settings.staging.{StagingSettings, StagingTableSettings}

class TestStagingSettings extends StagingSettings:
  override val icebergCatalog: IcebergStagingSettings = IcebergCatalogInfo.defaultIcebergStagingSettings

  override val table: StagingTableSettings = TestStagingTableSettings

object TestStagingSettings:
  def apply(): StagingSettings = new TestStagingSettings()
