package com.sneaksanddata.arcane.framework
package services.storage.services.s3

import services.storage.models.s3.S3ClientSettings

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy
import software.amazon.awssdk.retries.api.BackoffStrategy
import software.amazon.awssdk.services.s3.S3Client

type ConfiguredS3Client = (client: S3Client, settings: S3ClientSettings)

object S3Util:
  def getS3Client(credentialsProvider: AwsCredentialsProvider, settings: Option[S3ClientSettings]): ConfiguredS3Client =
    val serviceClientSettings = settings.getOrElse(S3ClientSettings())
    val retryStrategy =
      AwsRetryStrategy
        .standardRetryStrategy()
        .toBuilder
        .maxAttempts(serviceClientSettings.retryMaxAttempts)
        .backoffStrategy(
          BackoffStrategy.exponentialDelay(serviceClientSettings.retryBaseDelay, serviceClientSettings.retryMaxDelay)
        )
        .circuitBreakerEnabled(true)
        .build()
    var builder =
      S3Client.builder().forcePathStyle(serviceClientSettings.usePathStyle).credentialsProvider(credentialsProvider)

    if (serviceClientSettings.region.isDefined)
      builder = builder.region(serviceClientSettings.region.get)

    if (serviceClientSettings.endpoint.isDefined)
      builder = builder.endpointOverride(serviceClientSettings.endpoint.get)

    (builder.overrideConfiguration(o => o.retryStrategy(retryStrategy)).build(), serviceClientSettings)
