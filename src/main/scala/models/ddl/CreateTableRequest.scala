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
  * @param properties
  *   Optional table properties applied at creation. Some properties can only be set when the table is created rather
  *   than updated afterwards - `format-version` is the notable one, and a schema holding a `variant` column needs it
  *   set to `3`, since variant is not part of the v2 spec.
  */
case class CreateTableRequest(
    name: String,
    schema: Schema,
    replace: Boolean,
    partitionSpec: Option[PartitionSpec],
    sortOrder: Option[SortOrder],
    parquetBloomFilterFields: Seq[String],
    properties: Map[String, String] = Map.empty
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

  /** Create a table using provided schema and table properties applied at creation time.
    * @return
    */
  def apply(name: String, schema: Schema, replace: Boolean, properties: Map[String, String]): CreateTableRequest =
    new CreateTableRequest(
      name = name,
      schema = schema,
      replace = replace,
      partitionSpec = None,
      sortOrder = None,
      parquetBloomFilterFields = Seq(),
      properties = properties
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
