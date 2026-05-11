package com.sneaksanddata.arcane.framework
package models.maintenance

case class JdbcSnapshotExpirationRequest(
    tableName: String,
    retentionThreshold: String
) extends MaintenanceRequest:
  def toSqlExpression: String =
    s"ALTER TABLE ${tableName} execute expire_snapshots(retention_threshold => '${retentionThreshold}')"

object JdbcSnapshotExpirationRequest:
  /** A request to expire outdated snapshots.
   *
   * @param tableName
   * The name of the table to optimize.
   * @param optimizeThreshold
   * The threshold for optimization.
   * @param retentionThreshold
   * The file size threshold.
   * @param batchCount
   * The current batch counter.
   */
  def apply(tableName: String,
            optimizeThreshold: Long,
            retentionThreshold: String,
            batchCount: Long): Option[JdbcSnapshotExpirationRequest] =

    require(optimizeThreshold > 0, "Optimize threshold must be greater than 0")

    if (batchCount + 1) % optimizeThreshold == 0 then
      Some(JdbcSnapshotExpirationRequest(tableName, retentionThreshold))
    else
      None  
