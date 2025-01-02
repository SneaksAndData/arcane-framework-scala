package com.sneaksanddata.arcane.framework
package services.storage.models.azure

import java.time.Duration

case class AzureBlobStorageReaderSettings(httpMaxRetries: Int, httpRetryTimeout: Duration, httpMinRetryDelay: Duration, httpMaxRetryDelay: Duration, maxResultsPerPage: Int)

object AzureBlobStorageReaderSettings:
  def apply(
             httpMaxRetries: Int = 3, 
             httpRetryTimeout: Duration = Duration.ofSeconds(60), 
             httpMinRetryDelay: Duration = Duration.ofMillis(500), 
             httpMaxRetryDelay: Duration = Duration.ofSeconds(3), 
             maxResultsPerPage: Int = 5000): AzureBlobStorageReaderSettings = new AzureBlobStorageReaderSettings(
    httpMaxRetries = httpMaxRetries, 
    httpRetryTimeout = httpRetryTimeout, 
    httpMinRetryDelay = httpMinRetryDelay, 
    httpMaxRetryDelay = httpMaxRetryDelay, 
    maxResultsPerPage = maxResultsPerPage
  )
