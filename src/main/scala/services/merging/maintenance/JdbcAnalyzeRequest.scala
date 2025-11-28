package com.sneaksanddata.arcane.framework
package services.merging.maintenance

import services.base.{ConditionallyApplicable, SqlExpressionConvertable}

/** A request to optimize a table.
  * @param tableName
  *   The name of the table to analyze.
  * @param optimizeThreshold
  *   The batch count threshold for analysis to trigger.
  * @param includedColumns
  *   The list of columns to collect extended statistics for. All columns will be used if empty
  * @param batchIndex
  *   The batch index.
  */
case class JdbcAnalyzeRequest(
    tableName: String,
    optimizeThreshold: Long,
    includedColumns: Seq[String],
    batchIndex: Long
):
  require(optimizeThreshold > 0, "Optimize threshold must be greater than 0")

/** @inheritdoc
  */
given SqlExpressionConvertable[JdbcAnalyzeRequest] with

  /** @inheritdoc
    */
  extension (request: JdbcAnalyzeRequest)
    def toSqlExpression: String =
      if request.includedColumns.nonEmpty then
        val columnList = request.includedColumns.map(col => s"'$col'").mkString(",")
        s"ANALYZE TABLE ${request.tableName} WITH (columns = ARRAY[$columnList]))"
      else s"ANALYZE TABLE ${request.tableName})"

  /** @inheritdoc
    */
  extension (request: JdbcAnalyzeRequest) def name: String = request.tableName

/** @inheritdoc
  */
given ConditionallyApplicable[JdbcAnalyzeRequest] with

  /** @inheritdoc
    */
  extension (request: JdbcAnalyzeRequest)
    def isApplicable: Boolean = (request.batchIndex + 1) % request.optimizeThreshold == 0
