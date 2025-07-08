package com.sneaksanddata.arcane.framework
package services.merging.maintenance

import services.base.{ConditionallyApplicable, SqlExpressionConvertable}

/** A request to run a snapshot expiration.
  * @param tableName
  *   The name of the table to optimize.
  * @param optimizeThreshold
  *   The threshold for optimization.
  * @param retentionThreshold
  *   The retention threshold.
  * @param batchIndex
  *   The batch number.
  */
case class JdbcSnapshotExpirationRequest(
    tableName: String,
    optimizeThreshold: Long,
    retentionThreshold: String,
    batchIndex: Long
)

/** @inheritdoc
  */
given SqlExpressionConvertable[JdbcSnapshotExpirationRequest] with

  /** @inheritdoc
    */
  extension (request: JdbcSnapshotExpirationRequest)
    def toSqlExpression: String =
      s"ALTER TABLE ${request.tableName} execute expire_snapshots(retention_threshold => '${request.retentionThreshold}')"

  /** @inheritdoc
    */
  extension (request: JdbcSnapshotExpirationRequest) def name: String = request.tableName

/** @inheritdoc
  */
given ConditionallyApplicable[JdbcSnapshotExpirationRequest] with

  /** @inheritdoc
    */
  extension (request: JdbcSnapshotExpirationRequest)
    def isApplicable: Boolean = (request.batchIndex + 1) % request.optimizeThreshold == 0
