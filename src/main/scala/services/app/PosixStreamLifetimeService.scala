package com.sneaksanddata.arcane.framework
package services.app

import services.app.base.{InterruptionToken, StreamLifetimeService}

import zio.ZLayer

/**
 * A service that manages the lifetime of a stream on a POSIX-compliant system.
 */
class PosixStreamLifetimeService extends StreamLifetimeService with InterruptionToken:

  @volatile
  private var isCancelled = false
  
  @volatile
  private var isInterrupted = false

  /**
   * Returns true if the stream should be cancelled.
   */
  def cancelled: Boolean = this.isCancelled
  
  /**
   * Returns true if the stream has been interrupted.
   */
  def interrupted: Boolean = this.isInterrupted

  /**
   * Cancels the stream.
   */
  def cancel(): Unit = this.isCancelled = true
  
  /**
   * Starts watching for interruptions.
   */
  def start() :Unit = sys.addShutdownHook({
    this.isInterrupted = true
    cancel()
  })


object PosixStreamLifetimeService:

  /**
   * The ZLayer for the POSIX stream lifetime service.
   */
  val layer: ZLayer[Any, Nothing, StreamLifetimeService & InterruptionToken] =
    ZLayer.succeed(new PosixStreamLifetimeService)
