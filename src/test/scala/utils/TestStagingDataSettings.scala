package com.sneaksanddata.arcane.framework
package utils
import models.settings.StagingDataSettings

object TestStagingDataSettings extends StagingDataSettings:
  override val stagingCatalogName: String = ???
  override val stagingSchemaName: String = ???
  override val stagingTablePrefix = "staging_"
