package com.sneaksanddata.arcane.framework
package models.ddl

import org.apache.iceberg.types.{Type, Types}
import org.apache.iceberg.{PartitionSpec, Schema, SortOrder}

import scala.jdk.CollectionConverters.*

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
):

  /** The properties the table is actually created with.
    *
    * A `variant` column only exists from format version 3 on, and the format version cannot be raised after creation,
    * so a schema carrying one is pinned to v3 here rather than at each call site. Doing it centrally keeps staging,
    * backfill and target tables consistent with each other: a variant column in the target implies one in every
    * intermediate table the batch passes through, and missing a single site fails the whole stream at write time.
    *
    * An explicit `format-version` always wins, so a caller can still opt into a higher version.
    */
  val effectiveProperties: Map[String, String] =
    if properties.contains(CreateTableRequest.FormatVersionProperty) then properties
    else if CreateTableRequest.containsVariant(schema.asStruct()) then
      properties + (CreateTableRequest.FormatVersionProperty -> CreateTableRequest.VariantFormatVersion)
    else properties

object CreateTableRequest:

  /** Iceberg table property selecting the spec version. Only settable at creation. */
  val FormatVersionProperty = "format-version"

  /** Lowest format version that admits a `variant` column. */
  val VariantFormatVersion = "3"

  /** Whether the type, or anything nested inside it, is a variant. */
  private[ddl] def containsVariant(icebergType: Type): Boolean = icebergType match
    case _: Types.VariantType => true
    case nested: Type.NestedType =>
      nested.fields().asScala.exists(field => containsVariant(field.`type`()))
    case _ => false

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
