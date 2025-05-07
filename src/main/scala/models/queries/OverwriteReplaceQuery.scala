package com.sneaksanddata.arcane.framework
package models.queries

import models.settings.TablePropertiesSettings

/**
 * Represents an SQL query used to replace ALL data in the target table.
 * This type of query uses the CREATE OR REPLACE TABLE statement to replace (or update) data in the target table.
 *
 * @param sourceQuery Query that provides data for replacement
 * @param targetName Target table name
 */
case class OverwriteReplaceQuery(sourceQuery: String, targetName: String, tableProperties: TablePropertiesSettings) extends OverwriteQuery:
  def query: String =
      s"""CREATE OR REPLACE TABLE $targetName ${tableProperties.serializeToWithExpression} AS
         |$sourceQuery""".stripMargin

object OverwriteReplaceQuery:
  def apply(sourceQuery: String, targetName: String, tablePropertiesSettings: TablePropertiesSettings): OverwriteQuery =
    new OverwriteReplaceQuery(sourceQuery, targetName, tablePropertiesSettings)
