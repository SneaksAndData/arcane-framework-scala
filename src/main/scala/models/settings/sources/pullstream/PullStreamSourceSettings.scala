package com.sneaksanddata.arcane.framework
package models.settings.sources.pullstream

import upickle.ReadWriter
import upickle.implicits.key

import models.settings.sources.SourceSettings

/** DynamoDB pull stream source settings
  */
trait PullStreamSourceSettings extends SourceSettings:
  val pullIndexKey: String
  val pullIndexValue: String
  val versionFieldName: String
  val pageSize: Option[Int]
  val region: String
  val tableName: String
  val endpoint: Option[String]

  /** To use with JsonNode.at(String jsonPointer), applied to the `payload` attribute of each DynamoDB item before
    * decoding. Expects a JSON Pointer string, e.g. `/payload`, not a JSONPath expression. When empty, the pointer
    * published on the sink table by the producing service is used instead, and the document is decoded from its root
    * only when the table carries none either. Setting it here overrides the table.
    */
  val jsonPointerExpression: Option[String] = None
