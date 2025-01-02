package com.sneaksanddata.arcane.framework
package models.querygen

/**
 * Represents an SQL query used to replace ALL data in the target table
 * @param sourceQuery Query that provides data for replacement
 * @param targetName Target table name
 */
case class OverwriteQuery(sourceQuery: String, targetName: String) extends StreamingBatchQuery:
  def query: String =
      s"""INSERT OVERWRITE $targetName
         |$sourceQuery""".stripMargin

object OverwriteQuery:
  def apply(sourceQuery: String, targetName: String): OverwriteQuery = new OverwriteQuery(sourceQuery, targetName)
