package com.sneaksanddata.arcane.framework
package services.metrics.datadog

import services.metrics.datadog.base.DataDogClientBuilder

import com.timgroup.statsd.{NoOpStatsDClient, NonBlockingStatsDClientBuilder, StatsDClient}

/**
 * A trait that represents a DataDog configuration that does nothing.
 */
case class NoOpClientBuilder() extends DataDogClientBuilder:
  def build: StatsDClient = new NoOpStatsDClient()