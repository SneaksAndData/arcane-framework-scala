package com.sneaksanddata.arcane.framework
package models.settings.sources.pullstream

import upickle.ReadWriter
import upickle.implicits.key

import models.settings.sources.SourceSettings

/** Microsoft SQL Server database connection settings
  */
trait PullStreamSourceSettings extends SourceSettings:
  val pullIndexKey: String
  val pullIndexValue: String
  val watermarkFieldName: String
  val pageSize: Option[Int]
  val region: String
  val tableName: String
  val endpoint: Option[String]
