package com.sneaksanddata.arcane.framework
package models.settings

import models.settings.TableFormat.PARQUET

/** Data file format for Iceberg tables
  */
enum TableFormat:
  case PARQUET, ORC, AVRO

/** Provides table properties to apply for all tables managed by the stream Trino SQL API:
  * https://trino.io/docs/current/connector/iceberg.html#table-properties Use this to add any extra properties to be
  * supported by the streaming runner
  */
trait TablePropertiesSettings:
  /** Iceberg partitioning expressions to be used in CREATE TABLE ... WITH (partitioning = ARRAY[..], ...) Check out
    * Iceberg specification: https://iceberg.apache.org/spec/#partitioning Trino SQL API:
    * https://trino.io/docs/current/connector/iceberg.html#partitioned-tables
    */
  val partitionExpressions: Array[String]

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
        "partitioning"                 -> serializeArrayProperty(properties.partitionExpressions),
        "format"                       -> s"'${properties.format.toString}'",
        "sorted_by"                    -> serializeArrayProperty(properties.sortedBy),
        "parquet_bloom_filter_columns" -> serializeArrayProperty(properties.parquetBloomFilterColumns)
      )

  /** Extracts fields from partition expressions: year(col) -> col
    */
  extension (properties: TablePropertiesSettings)
    def partitionFields: Array[String] = properties.partitionExpressions
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

/** Empty settings to be used with no-op batches
  */
case object EmptyTablePropertiesSettings extends TablePropertiesSettings:

  override val partitionExpressions: Array[String] = Array.empty

  override val format: TableFormat = PARQUET

  override val sortedBy: Array[String] = Array.empty

  override val parquetBloomFilterColumns: Array[String] = Array.empty
