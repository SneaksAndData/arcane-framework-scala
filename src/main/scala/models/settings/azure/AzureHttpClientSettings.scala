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

case class DefaultAzureHttpClientSettings(
    override val httpMaxRetries: Int,
    override val httpMaxRetryDelay: Duration,
    override val httpMinRetryDelay: Duration,
    override val httpRetryTimeout: Duration,
    override val maxResultsPerPage: Int
) extends AzureHttpClientSettings derives ReadWriter
