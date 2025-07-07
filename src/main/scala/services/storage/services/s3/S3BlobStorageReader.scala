package com.sneaksanddata.arcane.framework
package services.storage.services.s3

import services.storage.models.s3.{S3ClientSettings, S3StoragePath}

import com.sneaksanddata.arcane.framework.services.storage.base.BlobStorageReader
import com.sneaksanddata.arcane.framework.services.storage.models.base.{BlobPath, StoredBlob}
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy
import software.amazon.awssdk.retries.api.BackoffStrategy
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import zio.{Task, ZIO}
import zio.stream.ZStream

import java.io.BufferedReader

final class S3BlobStorageReader(settings: Option[S3ClientSettings]) extends BlobStorageReader[S3StoragePath]:
  private val serviceClientSettings = settings.getOrElse(S3ClientSettings())
  private val defaultCredential     = DefaultCredentialsProvider.builder().asyncCredentialUpdateEnabled(true).build()
  private val retryStrategy =
    AwsRetryStrategy
      .standardRetryStrategy()
      .toBuilder
      .maxAttempts(serviceClientSettings.retryMaxAttempts)
      .backoffStrategy(
        BackoffStrategy.exponentialDelay(serviceClientSettings.retryBaseDelay, serviceClientSettings.retryMaxDelay)
      )
      .circuitBreakerEnabled(true)
      .build()
  private val s3Client: S3Client = {
    var builder = S3Client
      .builder()
      .credentialsProvider(defaultCredential)
      .forcePathStyle(serviceClientSettings.usePathStyle)
      .crossRegionAccessEnabled(true)
      .dualstackEnabled(true)
      .overrideConfiguration(o => o.retryStrategy(retryStrategy))

    if (serviceClientSettings.region.isDefined)
      builder = builder.region(serviceClientSettings.region.get)

    if (serviceClientSettings.endpoint.isDefined)
      builder = builder.endpointOverride(serviceClientSettings.endpoint.get)

    builder.build()
  }

  override def blobExists(blobPath: S3StoragePath): Task[Boolean] = ZIO
    .attemptBlocking(
      s3Client.headObject(HeadObjectRequest.builder().bucket(blobPath.bucket).key(blobPath.objectKey).build())
    )
    .flatMap(result => ZIO.logDebug(s"Blob ${blobPath.toHdfsPath} exists: ${result.eTag()}") *> ZIO.succeed(true))
    .onError(_ => ZIO.logDebug(s"Blob ${blobPath.toHdfsPath} does not exist") *> ZIO.succeed(false))

  override def readBlobContent(blobPath: S3StoragePath): Task[String] = ???

  override def streamBlobContent(blobPath: S3StoragePath): Task[BufferedReader] = ???

  override def streamPrefixes(rootPrefix: S3StoragePath): ZStream[Any, Throwable, StoredBlob] = ???
