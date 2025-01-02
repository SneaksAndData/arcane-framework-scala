package com.sneaksanddata.arcane.framework
package models.querygen

/**
 * Marker trait, represents a query used to process a streaming batch
 */
trait StreamingBatchQuery:
  def query: String
