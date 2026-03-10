package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.staging.StagingTableSettings

object TestStagingTableSettings extends StagingTableSettings:
  override val stagingCatalogName: String  = "catalog"
  override val stagingSchemaName: String   = "schema"
  override val stagingTablePrefix          = "staging_"
  override val isUnifiedSchema: Boolean    = false
  override val maxRowsPerFile: Option[Int] = Some(10000)
