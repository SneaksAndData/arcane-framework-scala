package com.sneaksanddata.arcane.framework
package models.settings.sources.blob

import services.storage.models.s3.S3ClientSettings

import upickle.ReadWriter
import upickle.implicits.key

/** Json source specific source settings
  */
trait JsonBlobSourceSettings extends BlobSourceSettings:
  /** Schema string for the json source in Avro format
    */
  val avroSchemaString: String

  /** To use with JsonNode.at(String jsonPointer). at() expects a JSON Pointer string, which is a different
    * specification than JSONPath. It requires forward slashes (/) to delineate segments and uses indexes for arrays.
    * Example: /store/book/0/title instead of $.store.book[0].title If an empty string is provided, pointer expression
    * will not be applied to the root node.
    */
  val jsonPointerExpression: String

  /** Json source can automatically explode array fields into additional rows. Out map key should contain a json pointer
    * string to the json array field. Inner map links array property names with field names in Avro Schema. Given fields
    * in Avro Schema: colA, colB, colC and source json: { "rootColA": "abc", "lines": [{ "a":1, "b":2, "c": 3}, { "a":1,
    * "b":2, "c": 3}] } the value for this setting should be: Map("/lines" -> Map("a" -> "colA", "b" -> "colB", "c" ->
    * "colC"))
    *
    * If not provided, nested and array fields will be recorded as Iceberg Variant type (ObjectType in Arcane) and
    * target table will require readers to support Iceberg V3 or later.
    */
  val jsonArrayPointers: Map[String, Map[String, String]]

case class DefaultJsonBlobSourceSettings(
    override val avroSchemaString: String,
    override val jsonPointerExpression: String,
    override val jsonArrayPointers: Map[String, Map[String, String]],
    override val primaryKeys: List[String],
    override val sourcePath: String,
    override val tempStoragePath: String,
    @key("s3") override val s3ClientSettings: S3ClientSettings
) extends JsonBlobSourceSettings derives ReadWriter
