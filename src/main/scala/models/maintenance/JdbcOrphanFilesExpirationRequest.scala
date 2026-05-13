package com.sneaksanddata.arcane.framework
package models.maintenance

case class JdbcOrphanFilesExpirationRequest(
    tableName: String,
    retentionThreshold: String
) extends MaintenanceRequest:
  def toSqlExpression: String =
    s"ALTER TABLE $tableName execute remove_orphan_files(retention_threshold => '$retentionThreshold')"

object JdbcOrphanFilesExpirationRequest:
  /** A request to remove orphan files.
    *
    * @param tableName
    *   The name of the table to optimize.
    * @param optimizeThreshold
    *   The threshold for optimization.
    * @param retentionThreshold
    *   The file size threshold.
    * @param batchCount
    *   The current batch counter.
    */
  def apply(
      tableName: String,
      optimizeThreshold: Long,
      retentionThreshold: String,
      batchCount: Long
  ): Option[JdbcOrphanFilesExpirationRequest] =

    require(optimizeThreshold > 0, "Optimize threshold must be greater than 0")

    if (batchCount + 1) % optimizeThreshold == 0 then
      Some(JdbcOrphanFilesExpirationRequest(tableName, retentionThreshold))
    else None
