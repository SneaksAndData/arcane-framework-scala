package com.sneaksanddata.arcane.framework
package models.settings.blob

/** Parquet source specific source settings
  */
trait ParquetBlobSourceSettings extends BlobSourceSettings:
  /** Enable column projection using field names instead of field ids:
    * https://iceberg.apache.org/spec/?column-projection#column-projection This is identical to using
    * `schema.name-mapping.default` with other Iceberg clients. Arcane with automatically generate field mapping from
    * the source Parquet file. Note that this might negatively interact with existing column mapping on the file, if
    * present. In most cases you do not need to enable this, unless a source Parquet file is written by code that
    * doesn't assign field identifiers.
    */
  val useNameMapping: Boolean

  /** Optional schema for the source. If provided, must contain base64-encoded bytes of an empty parquet file with the
    * matching schema. If not provided, schema will be inferred from a random file in the bucket.
    */
  val sourceSchema: Option[String]
