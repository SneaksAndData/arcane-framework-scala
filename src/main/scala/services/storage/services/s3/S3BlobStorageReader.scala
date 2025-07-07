package com.sneaksanddata.arcane.framework
package services.storage.services.s3

import logging.ZIOLogAnnotations.zlog
import services.storage.base.BlobStorageReader
import services.storage.models.base.StoredBlob
import services.storage.models.s3.S3ModelConversions.given
import services.storage.models.s3.{S3ClientSettings, S3StoragePath}

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy
import software.amazon.awssdk.retries.api.BackoffStrategy
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, HeadObjectRequest, ListObjectsV2Request}
import zio.stream.ZStream
import zio.{Task, ZIO}

import java.io.{BufferedReader, InputStreamReader}
import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions

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

  override def readBlobContent(blobPath: S3StoragePath): Task[String] = for
    _ <- zlog("Reading file %s/%s from S3", blobPath.bucket, blobPath.objectKey)
    response <- ZIO.attemptBlocking(
      s3Client.getObject(GetObjectRequest.builder().bucket(blobPath.bucket).key(blobPath.objectKey).build())
    )
    content <- ZIO.succeed(response.readAllBytes().map(_.toChar).mkString)
  yield content

  override def streamBlobContent(blobPath: S3StoragePath): Task[BufferedReader] = for
    _ <- zlog("Streaming file %s/%s from S3", blobPath.bucket, blobPath.objectKey)
    streamReader <- ZIO
      .attemptBlocking(
        s3Client.getObject(GetObjectRequest.builder().bucket(blobPath.bucket).key(blobPath.objectKey).build())
      )
      .map(InputStreamReader(_))
      .map(sr => new BufferedReader(sr))
  yield streamReader

  override def streamPrefixes(rootPrefix: S3StoragePath): ZStream[Any, Throwable, StoredBlob] = ZStream
    .paginate(
      s3Client.listObjectsV2(
        ListObjectsV2Request
          .builder()
          .bucket(rootPrefix.bucket)
          .prefix(rootPrefix.objectKey)
          .maxKeys(serviceClientSettings.maxResultsPerPage)
          .build()
      )
    ) { response =>
      if (response.isTruncated()) {
        (
          response.contents().asScala.toList,
          Some(
            s3Client.listObjectsV2(
              ListObjectsV2Request
                .builder()
                .bucket(rootPrefix.bucket)
                .prefix(rootPrefix.objectKey)
                .maxKeys(serviceClientSettings.maxResultsPerPage)
                .continuationToken(response.continuationToken())
                .build()
            )
          )
        )
      } else {
        (response.contents().asScala.toList, None)
      }
    }
    .flatMap(v => ZStream.fromIterable(v.map(implicitly)))
