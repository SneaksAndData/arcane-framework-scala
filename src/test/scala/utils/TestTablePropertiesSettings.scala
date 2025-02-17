package com.sneaksanddata.arcane.framework
package utils

import models.settings.{TableFormat, TablePropertiesSettings}

object TestTablePropertiesSettings extends TablePropertiesSettings:
  override val partitionExpressions: Array[String] = Array()
  override val format: TableFormat = TableFormat.PARQUET
  override val sortedBy: Array[String] = Array()
  override val parquetBloomFilterColumns: Array[String] = Array()

