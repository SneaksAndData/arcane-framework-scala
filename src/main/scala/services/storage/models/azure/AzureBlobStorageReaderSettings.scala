package com.sneaksanddata.arcane.framework
package services.storage.models.azure

import java.time.Duration

case class AzureBlobStorageReaderSettings(
    httpMaxRetries: Int,
    httpRetryTimeout: Duration,
    httpMinRetryDelay: Duration,
    httpMaxRetryDelay: Duration,
    maxResultsPerPage: Int
)

object AzureBlobStorageReaderSettings:
  def apply(
      httpMaxRetries: Int,
      httpRetryTimeout: Duration,
      httpMinRetryDelay: Duration,
      httpMaxRetryDelay: Duration,
      maxResultsPerPage: Int
  ): AzureBlobStorageReaderSettings = new AzureBlobStorageReaderSettings(
    httpMaxRetries = httpMaxRetries,
    httpRetryTimeout = httpRetryTimeout,
    httpMinRetryDelay = httpMinRetryDelay,
    httpMaxRetryDelay = httpMaxRetryDelay,
    maxResultsPerPage = maxResultsPerPage
  )

  val default =
    AzureBlobStorageReaderSettings(3, Duration.ofSeconds(60), Duration.ofMillis(500), Duration.ofSeconds(3), 5000)
