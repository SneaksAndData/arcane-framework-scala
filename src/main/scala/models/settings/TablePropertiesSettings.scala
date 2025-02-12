package com.sneaksanddata.arcane.framework
package models.settings

/**
 * Data file format for Iceberg tables
 */
enum TableFormat:
  case PARQUET, ORC, AVRO

/**
 * Provides table properties to apply for all tables managed by the stream
 * Trino SQL API: https://trino.io/docs/current/connector/iceberg.html#table-properties
 * Use this to add any extra properties to be supported by the streaming runner
 */
trait TablePropertiesSettings:
  /**
   * Iceberg partitioning expressions to be used in CREATE TABLE ... WITH (partitioning = ARRAY[..], ...)
   * Check out Iceberg specification: https://iceberg.apache.org/spec/#partitioning
   * Trino SQL API: https://trino.io/docs/current/connector/iceberg.html#partitioned-tables
   */
  val partitionExpressions: Array[String]

  /**
   * Optionally specifies the format of table data files; either PARQUET, ORC, or AVRO. Defaults to the value of the iceberg.file-format catalog configuration property, which defaults to PARQUET.
   *
   */
  val format: TableFormat

  /**
   *
   * The sort order to be applied during writes to the content of each file written to the table.
   * If the table files are sorted by columns c1 and c2, the sortedBy should be ["c1", "c2"]
   * The sort order applies to the contents written within each output file independently and not the entire dataset.
   */
  val sortedBy: Array[String]

  /**
   * List of columns to use for Parquet bloom filter. It improves the performance of queries using Equality and IN predicates when reading Parquet files. Requires Parquet format
   */
  val parquetBloomFilterColumns: Array[String]
