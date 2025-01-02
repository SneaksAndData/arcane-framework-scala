package com.sneaksanddata.arcane.framework
package models.querygen

/**
 * Represents an SQL query used to create a target table from the data provided
 */
case class InitializeQuery(sourceQuery: String, targetName: String) extends StreamingBatchQuery:
  def query: String =
    s"""CREATE TABLE $targetName AS $sourceQuery""".stripMargin


object InitializeQuery:
  def apply(sourceQuery: String, targetName: String): InitializeQuery = new InitializeQuery(sourceQuery, targetName)
