package com.sneaksanddata.arcane.framework
package models.settings.sources.blob

import services.storage.models.s3.S3ClientSettings

import upickle.ReadWriter
import upickle.implicits.key

/** Json source specific source settings
  */
trait JsonBlobSourceSettings extends BlobSourceSettings:
  /** Schema string for the JSON source in Avro format
    */
  val avroSchemaString: String

  /** To use with JsonNode.at(String jsonPointer). at() expects a JSON Pointer string, which is a different
    * specification than JSONPath. It requires forward slashes (/) to delineate segments and uses indexes for arrays.
    * Example: /store/book/0/title instead of $.store.book[0].title If an empty string or null is provided, pointer
    * expression will not be applied to the root node.
    */
  val jsonPointerExpression: Option[String]

case class DefaultJsonBlobSourceSettings(
    override val avroSchemaString: String,
    override val primaryKeys: List[String],
    override val sourcePath: String,
    override val shardStoragePath: String,
    override val tempStoragePath: String,
    override val jsonPointerExpression: Option[String] = None,
    @key("s3") override val s3ClientSettings: S3ClientSettings
) extends JsonBlobSourceSettings derives ReadWriter
