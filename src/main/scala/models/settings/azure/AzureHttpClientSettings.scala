package com.sneaksanddata.arcane.framework
package models.settings.azure

import models.serialization.JavaDurationRW.*

import upickle.ReadWriter

import java.time.Duration

trait AzureHttpClientSettings:
  /** Max retry count for http calls to Azure API
    */
  val httpMaxRetries: Int

  /** Timeout for http retries
    */
  val httpRetryTimeout: Duration

  /** Minimum delay for exp backoff
    */
  val httpMinRetryDelay: Duration

  /** Maximum delay for exp backoff
    */
  val httpMaxRetryDelay: Duration

  /** Max results per page in paginated API responses
    */
  val maxResultsPerPage: Int

  /** Optional forward proxy for all calls to Azure API. If not set, no proxy is used and proxy settings from the
    * environment (HTTP_PROXY, system properties) are ignored.
    */
  val proxy: Option[AzureProxySettings]

/** Forward proxy settings
  * @param host
  *   Proxy host
  * @param port
  *   Proxy port
  * @param nonProxyHosts
  *   Optional `|`-separated list of hosts that should bypass the proxy, for example `localhost|*.internal.net`
  */
case class AzureProxySettings(host: String, port: Int, nonProxyHosts: Option[String] = None) derives ReadWriter

case class DefaultAzureHttpClientSettings(
    override val httpMaxRetries: Int,
    override val httpMaxRetryDelay: Duration,
    override val httpMinRetryDelay: Duration,
    override val httpRetryTimeout: Duration,
    override val maxResultsPerPage: Int,
    override val proxy: Option[AzureProxySettings] = None
) extends AzureHttpClientSettings derives ReadWriter
