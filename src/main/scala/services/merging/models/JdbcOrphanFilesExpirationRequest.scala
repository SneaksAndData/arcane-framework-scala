package com.sneaksanddata.arcane.framework
package services.merging.models

import services.base.{ConditionallyApplicable, SqlExpressionConvertable}

/**
 * A request to run orphan files expiration.
 * @param tableName The name of the table to optimize.
 * @param optimizeThreshold The threshold for optimization.
 * @param retentionThreshold The retention threshold.
 * @param batchIndex The batch number.
 */
case class JdbcOrphanFilesExpirationRequest(tableName: String, optimizeThreshold: Long, retentionThreshold: String, batchIndex: Long)

/**
 * @inheritdoc
 */
given SqlExpressionConvertable[JdbcOrphanFilesExpirationRequest] with

  /**
   * @inheritdoc
   */
  extension (request: JdbcOrphanFilesExpirationRequest) def toSqlExpression: String =
    s"ALTER TABLE ${request.tableName} execute remove_orphan_files(retention_threshold => '${request.retentionThreshold}')"

  /**
   * @inheritdoc
   */
  extension (request: JdbcOrphanFilesExpirationRequest) def name: String = request.tableName

/**
 * @inheritdoc
 */
given ConditionallyApplicable[JdbcOrphanFilesExpirationRequest] with

  /**
   * @inheritdoc
   */
  extension (request: JdbcOrphanFilesExpirationRequest) def isApplicable: Boolean = (request.batchIndex + 1) % request.optimizeThreshold == 0


