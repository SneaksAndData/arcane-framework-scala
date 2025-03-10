package com.sneaksanddata.arcane.framework
package services.streaming.base

import services.streaming.processors.transformers.StagingProcessor

trait HookManager:
  
  /**
   *  Enriches received staging batch with metadata and converts it to in-flight batch.
   **/
  def toInFlightBatch: StagingProcessor#ToInFlightBatch
