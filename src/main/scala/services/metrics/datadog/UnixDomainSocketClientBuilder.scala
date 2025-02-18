package com.sneaksanddata.arcane.framework
package services.metrics.datadog

import services.metrics.datadog.base.DataDogClientBuilder

import com.timgroup.statsd.{NonBlockingStatsDClientBuilder, StatsDClient}

/**
 * A trait that represents a DataDog configuration that uses a unix domain socket.
 */
class UnixDomainSocketClientBuilder(statsdServerName: String, environment: String, serviceVersion: String, prefix: String)
  extends DataDogClientBuilder:

  /**
   * Builds a DataDog client.
   *
   * @return The DataDog client.
   */
  def build: StatsDClient = new NonBlockingStatsDClientBuilder()
    .hostname("/var/run/datadog/dsd.socket")
    .port(0) // Necessary for unix socket
    .prefix(prefix)
    .build();