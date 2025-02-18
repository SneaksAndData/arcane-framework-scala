package com.sneaksanddata.arcane.framework
package services.base

import services.metrics.datadog.{NoOpClientBuilder, UnixDomainSocketClientBuilder}

import com.timgroup.statsd.StatsDClient


/**
 * A trait that represents a DataDog configuration.
 */
trait DataDogClientBuilder:

  /**
   * Builds a DataDog client.
   *
   * @return The DataDog client.
   */
  def build: StatsDClient

/**
 * The companion object for the DataDogConfiguration class.
 */
object DataDogClientBuilder:

  /**
   * Selects the DataDog configuration based on the environment.
   *
   * @param metricNamespace The metric namespace.
   * @return The DataDog configuration.
   */
  def selectFromEnvironment(metricNamespace: String): DataDogClientBuilder = sys.env.get("ARCANE_FRAMEWORK__DATADOG_ENABLED") match
    case Some(_) => unixDomainSocket(metricNamespace)
    case None => noOp(metricNamespace)

  private def noOp(metricNamespace: String): DataDogClientBuilder =
    NoOpClientBuilder()

  private def unixDomainSocket(metricNamespace: String): DataDogClientBuilder =
    val path = sys.env.getOrElse("ARCANE_FRAMEWORK__DD_UNIX_DOMAIN_SOCKET_PATH", "unix:///var/run/datadog/dsd.socket")
    val deployEnv = sys.env.getOrElse("ARCANE_FRAMEWORK__DD_DEPLOY_ENV", "development")
    val version = sys.env.getOrElse("APPLICATION_VERSION", "0.0.0")
    UnixDomainSocketClientBuilder(path, deployEnv, version, metricNamespace.toLowerCase)