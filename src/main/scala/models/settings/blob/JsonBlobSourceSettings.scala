package com.sneaksanddata.arcane.framework
package models.settings.blob

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
