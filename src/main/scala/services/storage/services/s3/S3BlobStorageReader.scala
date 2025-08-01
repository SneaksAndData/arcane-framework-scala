package com.sneaksanddata.arcane.framework
package services.storage.services.s3

import logging.ZIOLogAnnotations.{zlog, zlogStream}
import services.storage.base.BlobStorageReader
import services.storage.models.base.StoredBlob
import services.storage.models.s3.S3ModelConversions.given
import services.storage.models.s3.{S3ClientSettings, S3StoragePath}

import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials,
  AwsCredentials,
  AwsCredentialsProvider,
  DefaultCredentialsProvider,
  StaticCredentialsProvider
}
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy
import software.amazon.awssdk.retries.api.BackoffStrategy
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{
  GetObjectRequest,
  HeadObjectRequest,
  ListObjectsV2Request,
  NoSuchKeyException
}
import zio.stream.{ZSink, ZStream}
import zio.{Task, ZIO}

import java.io.{BufferedReader, ByteArrayInputStream, InputStreamReader}
import java.nio.file.Paths
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions

final class S3BlobStorageReader(
    credentialsProvider: AwsCredentialsProvider,
    settings: Option[S3ClientSettings]
) extends BlobStorageReader[S3StoragePath]:
  private val serviceClientSettings = settings.getOrElse(S3ClientSettings())
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
    var builder =
      S3Client.builder().forcePathStyle(serviceClientSettings.usePathStyle).credentialsProvider(credentialsProvider)

    if (serviceClientSettings.region.isDefined)
      builder = builder.region(serviceClientSettings.region.get)

    if (serviceClientSettings.endpoint.isDefined)
      builder = builder.endpointOverride(serviceClientSettings.endpoint.get)

    builder.overrideConfiguration(o => o.retryStrategy(retryStrategy)).build()
  }

  override def blobExists(blobPath: S3StoragePath): Task[Boolean] = ZIO
    .attemptBlocking(
      s3Client.headObject(HeadObjectRequest.builder().bucket(blobPath.bucket).key(blobPath.objectKey).build())
    )
    .flatMap(result => ZIO.logDebug(s"Blob ${blobPath.toHdfsPath} exists: ${result.eTag()}") *> ZIO.succeed(true))
    .catchSome { case _: NoSuchKeyException =>
      ZIO.logDebug(s"Blob ${blobPath.toHdfsPath} does not exist") *> ZIO.succeed(false)
    }

  override def readBlobContent(blobPath: S3StoragePath): Task[String] = for
    _ <- zlog("Reading file %s/%s from S3", blobPath.bucket, blobPath.objectKey)
    response <- ZIO.attemptBlocking(
      s3Client.getObject(GetObjectRequest.builder().bucket(blobPath.bucket).key(blobPath.objectKey).build())
    )
    content <- ZIO.succeed(response.readAllBytes().map(_.toChar).mkString)
  yield content

  override def streamBlobContent(blobPath: S3StoragePath): Task[BufferedReader] = for
    _ <- zlog("Streaming file %s/%s as text from S3", blobPath.bucket, blobPath.objectKey)
    streamReader <- ZIO
      .attemptBlocking(
        s3Client.getObject(GetObjectRequest.builder().bucket(blobPath.bucket).key(blobPath.objectKey).build())
      )
      .map(InputStreamReader(_))
      .map(sr => new BufferedReader(sr))
  yield streamReader

  private def preBuildListObjectsV2Request(
      path: S3StoragePath,
      maxKeys: Int = serviceClientSettings.maxResultsPerPage
  ): ListObjectsV2Request.Builder =
    ListObjectsV2Request
      .builder()
      .bucket(path.bucket)
      .prefix(path.objectKey)
      .maxKeys(maxKeys)

  override def streamPrefixes(rootPrefix: S3StoragePath): ZStream[Any, Throwable, StoredBlob] = ZStream
    .paginate(
      s3Client.listObjectsV2(preBuildListObjectsV2Request(rootPrefix).build())
    ) { response =>
      if (response.isTruncated()) {
        (
          response.contents().asScala.toList,
          Some(
            s3Client.listObjectsV2(
              preBuildListObjectsV2Request(rootPrefix)
                .continuationToken(response.nextContinuationToken())
                .build()
            )
          )
        )
      } else {
        (response.contents().asScala.toList, None)
      }
    }
    .map(objects => objects.map((rootPrefix.bucket, _)))
    .flatMap(v => ZStream.fromIterable(v.map(implicitly)))

  override def streamBlob(blobPath: S3StoragePath): ZStream[Any, Throwable, Byte] =
    zlogStream("Streaming file %s/%s content (bytes) from S3", blobPath.bucket, blobPath.objectKey).flatMap(_ =>
      ZStream
        .fromZIO(
          ZIO
            .attemptBlocking(
              s3Client.getObject(GetObjectRequest.builder().bucket(blobPath.bucket).key(blobPath.objectKey).build())
            )
        )
        .flatMap(ZStream.fromInputStream(_))
    )

  override def downloadBlob(blobPath: S3StoragePath, localPath: String): Task[String] = for
    fileName <- ZIO.succeed(s"$localPath/${UUID.randomUUID()}")
    _        <- zlog("Downloading blob %s to file %s", blobPath.objectKey, fileName)
    _        <- streamBlob(blobPath) >>> ZSink.fromPath(Paths.get(fileName))
  yield fileName

  override def downloadBlob(blobPath: String, localPath: String): Task[String] =
    downloadBlob(S3StoragePath(blobPath).get, localPath)

  /** Downloads a random blob found at a given path to a temporary folder
    *
    * @param rootPath
    *   The path containing one or more blob.
    * @return
    *   A path to the downloaded blob.
    */
  override def downloadRandomBlob(rootPath: S3StoragePath, localPath: String): Task[String] = for
    response <- ZIO.attemptBlocking(s3Client.listObjectsV2(preBuildListObjectsV2Request(rootPath, 1).build()))
    blob     <- ZIO.succeed(response.contents().asScala.toList.head)
    result   <- downloadBlob(S3StoragePath(bucket = rootPath.bucket, objectKey = blob.key()), localPath)
  yield result
