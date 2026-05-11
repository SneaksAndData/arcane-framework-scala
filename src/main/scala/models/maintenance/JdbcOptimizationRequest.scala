package com.sneaksanddata.arcane.framework
package models.maintenance

case class JdbcOptimizationRequest(tableName: String, fileSizeThreshold: String) extends MaintenanceRequest:
  def toSqlExpression: String =
    s"ALTER TABLE ${tableName} execute optimize(file_size_threshold => '${fileSizeThreshold}')"
   
object JdbcOptimizationRequest:
  /** A request to optimize a table.
   *
   * @param tableName
   * The name of the table to optimize.
   * @param optimizeThreshold
   * The threshold for optimization.
   * @param fileSizeThreshold
   * The file size threshold.
   * @param batchCount
   * The current batch counter.
   */
  def apply(tableName: String,
            optimizeThreshold: Long,
            fileSizeThreshold: String,
            batchCount: Long): Option[JdbcOptimizationRequest] =
    
    require(optimizeThreshold > 0, "Optimize threshold must be greater than 0")
    
    if (batchCount + 1) % optimizeThreshold == 0 then
      Some(JdbcOptimizationRequest(tableName, fileSizeThreshold))
    else
      None  
