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
  val region: String
  val tableName: String
  val endpoint: Option[String]
