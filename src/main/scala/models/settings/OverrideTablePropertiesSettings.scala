package com.sneaksanddata.arcane.framework
package models.settings

import upickle.ReadWriter

/** A partial override of `TablePropertiesSettings` where every field is optional to support override/patch-style JSON
  * deserialization.
  */
trait OverrideTablePropertiesSettings:
  /** Optional override for the table file format.
    */
  val format: Option[TableFormat]

  /** Optional override for the sort columns applied to table files.
    */
  val sortedBy: Option[Array[String]]

  /** Optional override for the Parquet bloom filter columns.
    */
  val parquetBloomFilterColumns: Option[Array[String]]

/** Default implementation for `OverrideTablePropertiesSettings` using optional values.
  */
case class DefaultOverrideTablePropertiesSettings(
    override val format: Option[TableFormat] = None,
    override val sortedBy: Option[Array[String]] = None,
    override val parquetBloomFilterColumns: Option[Array[String]] = None
) extends OverrideTablePropertiesSettings derives ReadWriter
