package com.sneaksanddata.arcane.framework
package services.app.base

/**
 * Provides information about a stream interruption.
 */
trait InterruptionToken {
  /**
   * Returns true if the stream has been interrupted.
   */
  def interrupted: Boolean
}
