package com.sneaksanddata.arcane.framework
package models.settings

import models.settings.TableFormat.PARQUET

import upickle.ReadWriter
import upickle.default.*

/** Data file format for Iceberg tables
  */
enum TableFormat derives ReadWriter:
  case PARQUET, ORC, AVRO

/** Provides table properties to apply for all tables managed by the stream Trino SQL API:
  * https://trino.io/docs/current/connector/iceberg.html#table-properties Use this to add any extra properties to be
  * supported by the streaming runner
  */
trait TablePropertiesSettings:
  // TODO: https://github.com/SneaksAndData/arcane-framework-scala/issues/307

  /** Optionally specifies the format of table data files; either PARQUET, ORC, or AVRO. Defaults to the value of the
    * iceberg.file-format catalog configuration property, which defaults to PARQUET.
    */
  val format: TableFormat

  /** The sort order to be applied during writes to the content of each file written to the table. If the table files
    * are sorted by columns c1 and c2, the sortedBy should be ["c1", "c2"] The sort order applies to the contents
    * written within each output file independently and not the entire dataset.
    */
  val sortedBy: Array[String]

  /** List of columns to use for Parquet bloom filter. It improves the performance of queries using Equality and IN
    * predicates when reading Parquet files. Requires Parquet format
    */
  val parquetBloomFilterColumns: Array[String]

object TablePropertiesSettings:

  /** Serializes the table properties to a Trino with expression
    */
  extension (properties: TablePropertiesSettings)
    def serializeToWithExpression: String =
      val supportedProperties =
        properties.serializeToMap.map { (propertyKey, propertyValue) => s"$propertyKey=$propertyValue" }.mkString(", ")
      s"WITH ($supportedProperties)"

  /** Serializes the table properties to a HashMap
    */
  extension (properties: TablePropertiesSettings)
    def serializeToMap: Map[String, String] =
      Map(
        "partitioning"                 -> serializeArrayProperty(Array.empty[String]),
        "format"                       -> s"'${properties.format.toString}'",
        "sorted_by"                    -> serializeArrayProperty(properties.sortedBy),
        "parquet_bloom_filter_columns" -> serializeArrayProperty(properties.parquetBloomFilterColumns)
      )

  /** Extracts fields from partition expressions: year(col) -> col
    */
  extension (properties: TablePropertiesSettings)
    def partitionFields: Array[String] = Array
      .empty[String]
      .map { pe =>
        val matchingPart = partitionExpressionParts.filter(pep => pe.toLowerCase.startsWith(pep._1)).head
        pe.substring(pe.indexOf("(") + 1, pe.indexOf(matchingPart._2))
      }

  private def serializeArrayProperty(prop: Array[String]): String =
    val value = prop.map { expr => s"'$expr'" }.mkString(",")
    s"ARRAY[$value]"

  private val partitionExpressionParts: Map[String, String] = Map(
    "year"     -> ")",
    "month"    -> ")",
    "day"      -> ")",
    "hour"     -> ")",
    "bucket"   -> ",",
    "truncate" -> ","
  )

case class DefaultTablePropertiesSettings(
    override val format: TableFormat,
    override val sortedBy: Array[String],
    override val parquetBloomFilterColumns: Array[String]
) extends TablePropertiesSettings derives ReadWriter

/** Empty settings to be used with no-op batches
  */
case object EmptyTablePropertiesSettings extends TablePropertiesSettings:

  override val format: TableFormat = PARQUET

  override val sortedBy: Array[String] = Array.empty

  override val parquetBloomFilterColumns: Array[String] = Array.empty
