package com.sneaksanddata.arcane.framework
package models.maintenance

case class JdbcAnalyzeRequest(
    tableName: String,
    includedColumns: Seq[String]
) extends MaintenanceRequest:
  def toSqlExpression: String =
    if includedColumns.nonEmpty then
      val columnList = includedColumns.map(col => s"'$col'").mkString(",")
      s"ANALYZE ${tableName} WITH (columns = ARRAY[$columnList])"
    else s"ANALYZE ${tableName}"

object JdbcAnalyzeRequest:
  /** A request to analyze a table.
    *
    * @param tableName
    *   The name of the table to optimize.
    * @param optimizeThreshold
    *   Columns to run ANALYZE on.
    * @param includedColumns
    *   The file size threshold.
    * @param batchCount
    *   The current batch counter.
    */
  def apply(
      tableName: String,
      optimizeThreshold: Long,
      includedColumns: Seq[String],
      batchCount: Long
  ): Option[JdbcAnalyzeRequest] =

    require(optimizeThreshold > 0, "Optimize threshold must be greater than 0")

    if (batchCount + 1) % optimizeThreshold == 0 then Some(JdbcAnalyzeRequest(tableName, includedColumns))
    else None
