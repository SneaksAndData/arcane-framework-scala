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
  val watermarkFieldName: String
  val pageSize: Option[Int]
  val region: String
  val tableName: String
  val endpoint: Option[String]

  /** To use with JsonNode.at(String jsonPointer), applied to the `payload` attribute of each DynamoDB item before
    * decoding. Expects a JSON Pointer string, e.g. `/payload`, not a JSONPath expression. When empty, the document is
    * decoded from its root.
    */
  val jsonPointerExpression: Option[String] = None

  /** Hoists nested fields of the `payload` document up to the root so that they land in their own columns instead of
    * being stored as one JSON string. The outer map key is a JSON pointer to the nested node; the inner map renames
    * source field names to target column names, and applies to the root's own fields as well.
    *
    * Given the payload `{"id": "evt_001", "payload": {"eventType": "...", "source": "..."}}`, the setting
    * `Map("/payload" -> Map("id" -> "push_event_id"))` produces the columns `push_event_id`, `eventType` and `source`.
    *
    * An array node yields one row per element; an object node, such as an Avro `map`, yields a single row.
    */
  val jsonArrayPointers: Map[String, Map[String, String]] = Map()
