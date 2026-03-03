package com.sneaksanddata.arcane.framework
package models.ddl

import org.apache.iceberg.{PartitionSpec, Schema, SortOrder}

/** Model used by CatalogEntityManager to create tables.
  * @param name
  *   Desired name for the table
  * @param schema
  *   Schema for the table
  * @param replace
  *   Whether to replace or fail if table exists
  * @param partitionSpec
  *   Optional partition specification
  * @param sortOrder
  *   Optional sort order configuration
  * @param parquetBloomFilterFields
  *   Optional fields to include in Parquet Bloom filter
  */
case class CreateTableRequest(
    name: String,
    schema: Schema,
    replace: Boolean,
    partitionSpec: Option[PartitionSpec],
    sortOrder: Option[SortOrder],
    parquetBloomFilterFields: Seq[String]
)

object CreateTableRequest:
  /** Create a table using provided schema, replacing if exists, if requested
    * @return
    */
  def apply(name: String, schema: Schema, replace: Boolean): CreateTableRequest = new CreateTableRequest(
    name = name,
    schema = schema,
    replace = replace,
    partitionSpec = None,
    sortOrder = None,
    parquetBloomFilterFields = Seq()
  )

  /** Advanced: create a table with partitions, sort order and bloom filter activated
    * @return
    */
  def apply(
      name: String,
      schema: Schema,
      replace: Boolean,
      partitionSpec: Option[PartitionSpec],
      sortOrder: Option[SortOrder],
      parquetBloomFilterFields: Seq[String]
  ): CreateTableRequest = new CreateTableRequest(
    name,
    schema,
    replace,
    partitionSpec,
    sortOrder,
    parquetBloomFilterFields
  )
