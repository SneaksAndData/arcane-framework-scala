package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.{TableFormat, TablePropertiesSettings}

object TestTablePropertiesSettings extends TablePropertiesSettings:
  override val format: TableFormat                      = TableFormat.PARQUET
  override val sortedBy: Array[String]                  = Array()
  override val parquetBloomFilterColumns: Array[String] = Array()
  override type MergeableFrom = this.type
  override type MergeResult = this.type
  override def merge(overrides: Option[MergeableFrom]): MergeResult = ???

object CustomTablePropertiesSettings:
  def apply(partitions: Seq[String]): TablePropertiesSettings = new TablePropertiesSettings {
    override val parquetBloomFilterColumns: Array[String] = Array.empty
    override val format: TableFormat                      = TableFormat.PARQUET
    override val sortedBy: Array[String]                  = Array.empty
    override type MergeableFrom = this.type
    override type MergeResult = this.type
    override def merge(overrides: Option[MergeableFrom]): MergeResult = ???
  }
