package com.sneaksanddata.arcane.framework
package services.app.base

/**
 * A trait that represents a service that can be used to determine if a stream should be cancelled.
 */
trait StreamLifetimeService {
  /**
   * Returns true if the stream should be cancelled.
   */
  def cancelled: Boolean
  
  /**
   * Cancels the stream.
   */
  def cancel(): Unit
  
  /**
   * Starts the lifetime service.
   */
  def start(): Unit
}
