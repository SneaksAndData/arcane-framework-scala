package com.sneaksanddata.arcane.framework
package services.storage.services.s3

import services.storage.models.s3.{S3ClientSettings, S3StoragePath}

import com.sneaksanddata.arcane.framework.services.storage.base.BlobStorageReader
import com.sneaksanddata.arcane.framework.services.storage.models.base.{BlobPath, StoredBlob}
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy
import software.amazon.awssdk.services.s3.S3Client
import zio.Task
import zio.stream.ZStream

import java.io.BufferedReader

final class S3BlobStorageReader(settings: Option[S3ClientSettings]) extends BlobStorageReader[S3StoragePath]:
  private val serviceClientSettings = settings.getOrElse(S3ClientSettings())
  private val defaultCredential     = DefaultCredentialsProvider.builder().asyncCredentialUpdateEnabled(true).build()
  private val retryStrategy =
    AwsRetryStrategy.standardRetryStrategy().toBuilder.maxAttempts(serviceClientSettings.retryMaxAttempts)
  private val s3Client = {
    var builder = S3Client
      .builder()
      .credentialsProvider(defaultCredential)
      .forcePathStyle(serviceClientSettings.usePathStyle)
      .crossRegionAccessEnabled(true)
      .dualstackEnabled(true)

    if (serviceClientSettings.region.isDefined)
      builder = builder.region(serviceClientSettings.region.get)

    if (serviceClientSettings.endpoint.isDefined)
      builder = builder.endpointOverride(serviceClientSettings.endpoint.get)

    builder.build()
  }

  override def blobExists(blobPath: S3StoragePath): Task[Boolean] = ???

  override def readBlobContent(blobPath: BlobPath): Task[String] = ???

  override def streamBlobContent(blobPath: BlobPath): Task[BufferedReader] = ???

  override def streamPrefixes(rootPrefix: S3StoragePath): ZStream[Any, Throwable, StoredBlob] = ???
