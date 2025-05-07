package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.{TableFormat, TablePropertiesSettings}

object TestTablePropertiesSettings extends TablePropertiesSettings:
  override val partitionExpressions: Array[String]      = Array()
  override val format: TableFormat                      = TableFormat.PARQUET
  override val sortedBy: Array[String]                  = Array()
  override val parquetBloomFilterColumns: Array[String] = Array()

object CustomTablePropertiesSettings:
  def apply(partitions: Seq[String]): TablePropertiesSettings = new TablePropertiesSettings {
    override val partitionExpressions: Array[String]      = partitions.toArray
    override val parquetBloomFilterColumns: Array[String] = Array.empty
    override val format: TableFormat                      = TableFormat.PARQUET
    override val sortedBy: Array[String]                  = Array.empty
  }
