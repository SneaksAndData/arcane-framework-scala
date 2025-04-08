package com.sneaksanddata.arcane.framework
package services.merging.models

import services.base.{ConditionallyApplicable, SqlExpressionConvertable}

/**
 * A request to optimize a table.
 * @param tableName The name of the table to optimize.
 * @param optimizeThreshold The threshold for optimization.
 * @param fileSizeThreshold The file size threshold.
 * @param batchIndex The batch index.
 */
case class JdbcOptimizationRequest(tableName: String, optimizeThreshold: Long, fileSizeThreshold: String, batchIndex: Long):
  require(optimizeThreshold > 0, "Optimize threshold must be greater than 0")
  

/**
 * @inheritdoc
 */
given SqlExpressionConvertable[JdbcOptimizationRequest] with
  
  /**
   * @inheritdoc
   */
  extension (request: JdbcOptimizationRequest) def toSqlExpression: String =
    s"ALTER TABLE ${request.tableName} execute optimize(file_size_threshold => '${request.fileSizeThreshold}')"

  /**
   * @inheritdoc
   */
  extension (request: JdbcOptimizationRequest) def name: String = request.tableName

/**
 * @inheritdoc
 */
given ConditionallyApplicable[JdbcOptimizationRequest] with

  /**
   * @inheritdoc
   */
  extension (request: JdbcOptimizationRequest) def isApplicable: Boolean = 
    (request.batchIndex+1) % request.optimizeThreshold == 0
