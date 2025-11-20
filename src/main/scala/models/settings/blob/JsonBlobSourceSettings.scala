package com.sneaksanddata.arcane.framework
package models.settings.blob

/** Json source specific source settings
  */
trait JsonBlobSourceSettings extends BlobSourceSettings:
  /** Schema string for the json source in Avro format
    */
  val avroSchemaString: String
