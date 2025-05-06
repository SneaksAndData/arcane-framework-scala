package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.StagingDataSettings

object TestStagingDataSettings extends StagingDataSettings:
  override val stagingCatalogName: String = "catalog"
  override val stagingSchemaName: String = "schema"
  override val stagingTablePrefix = "staging_"
  override val isUnifiedSchema: Boolean = false
