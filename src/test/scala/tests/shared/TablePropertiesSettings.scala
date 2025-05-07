package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.TableFormat.PARQUET
import models.settings.{TableFormat, TablePropertiesSettings}

object TablePropertiesSettings extends TablePropertiesSettings:
  override val partitionExpressions: Array[String]      = Array()
  override val format: TableFormat                      = PARQUET
  override val sortedBy: Array[String]                  = Array()
  override val parquetBloomFilterColumns: Array[String] = Array()
