package com.sneaksanddata.arcane.framework
package utils
import models.settings.StagingDataSettings

object TestStagingDataSettings extends StagingDataSettings:
  override val stagingCatalogName: String = "catalog"
  override val stagingSchemaName: String = "schema"
  override val stagingTablePrefix: String = "staging_stream_id"
