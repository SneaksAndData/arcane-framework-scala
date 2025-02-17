package com.sneaksanddata.arcane.framework
package models.querygen

import models.settings.TablePropertiesSettings

/**
 * Represents an SQL query used to replace ALL data in the target table
 *
 * @param sourceQuery Query that provides data for replacement
 * @param targetName Target table name
 */
case class OverwriteMergeQuery(sourceQuery: String, targetName: String, tableProperties: TablePropertiesSettings) extends StreamingBatchQuery:
  def query: String =
      s"""CREATE OR REPLACE TABLE $targetName ${tableProperties.serializeToWithExpression} AS
         |$sourceQuery""".stripMargin

object OverwriteMergeQuery:
  def apply(sourceQuery: String, targetName: String, tablePropertiesSettings: TablePropertiesSettings): OverwriteReplaceQuery =
    new OverwriteReplaceQuery(sourceQuery, targetName, tablePropertiesSettings)
