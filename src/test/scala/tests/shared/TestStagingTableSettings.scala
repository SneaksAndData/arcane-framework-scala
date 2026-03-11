package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.staging.StagingTableSettings

object TestStagingTableSettings extends StagingTableSettings:
  override val stagingCatalogName: String  = "demo"
  override val stagingSchemaName: String   = "test"
  override val stagingTablePrefix          = "staging_table"
  override val isUnifiedSchema: Boolean    = false
  override val maxRowsPerFile: Option[Int] = Some(10000)
