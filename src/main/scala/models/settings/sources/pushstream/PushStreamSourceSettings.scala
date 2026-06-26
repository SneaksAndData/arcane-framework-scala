package com.sneaksanddata.arcane.framework
package models.settings.sources.pushstream

import upickle.ReadWriter
import upickle.implicits.key

/** Microsoft SQL Server database connection settings
 */
trait PushStreamSourceSettings:
  val sourceTableName: String
  // TODO: table names should be iceberg compliant {warehouse}.{namespace}.{tablename}
  val targetTableName: String
  val primaryKeyFieldName: String
  val primaryKeyValue: String
  val watermarkFieldName: String
  /** Fetch size for ResultSets.
   */
  val region: String
  val tableName: String
  val endpoint: Option[String]

