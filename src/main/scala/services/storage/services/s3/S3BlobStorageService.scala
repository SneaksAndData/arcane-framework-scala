package com.sneaksanddata.arcane.framework
package services.storage.services.s3

import logging.ZIOLogAnnotations.{zlog, zlogStream}
import services.storage.base.{BlobStorageReader, BlobStorageWriter}
import services.storage.models.base.StoredBlob
import services.storage.models.s3.S3ModelConversions.given
import services.storage.models.s3.{S3ClientSettings, S3StoragePath}

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.*
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, Task, ZIO}

import java.io.{BufferedReader, InputStreamReader}
import java.nio.file.Paths
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions

final class S3BlobStorageService(
    credentialsProvider: AwsCredentialsProvider,
    settings: Option[S3ClientSettings]
) extends BlobStorageReader[S3StoragePath]
    with BlobStorageWriter[S3StoragePath]:
  private val (s3Client, serviceClientSettings) = S3Util.getS3Client(credentialsProvider, settings)

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
    .paginateChunk(
      s3Client.listObjectsV2(preBuildListObjectsV2Request(rootPrefix).build())
    ) { response =>
      if (response.isTruncated()) {
        (
          Chunk(response.contents().asScala.map((rootPrefix.bucket, _)).map(implicitly)),
          Some(
            s3Client.listObjectsV2(
              preBuildListObjectsV2Request(rootPrefix)
                .continuationToken(response.nextContinuationToken())
                .build()
            )
          )
        )
      } else {
        (Chunk(response.contents().asScala.map((rootPrefix.bucket, _)).map(implicitly)), None)
      }
    }
    .flatMap(ZStream.fromIterable)

  override def streamPrefixes(rootPrefix: String): ZStream[Any, Throwable, StoredBlob] = ZStream
    .from(S3StoragePath.applySafe(rootPrefix).get)
    .flatMap(streamPrefixes)

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
  override def downloadRandomBlob(rootPath: S3StoragePath, localPath: String): Task[Option[String]] = for
    response <- ZIO.attemptBlocking(s3Client.listObjectsV2(preBuildListObjectsV2Request(rootPath, 1).build()))
    blob     <- ZIO.succeed(response.contents().asScala.toList.headOption)
    result <- blob match
      case Some(existingBlob) =>
        downloadBlob(S3StoragePath(bucket = rootPath.bucket, objectKey = existingBlob.key()), localPath).map(v =>
          Some(v)
        )
      case None => ZIO.succeed(Option.empty[String])
  yield result

  override def blobMetadata(blobPath: String): Task[StoredBlob] = for
    s3Path      <- ZIO.attempt(S3StoragePath.applySafe(blobPath).get)
    headRequest <- ZIO.succeed(HeadObjectRequest.builder().bucket(s3Path.bucket).key(s3Path.objectKey).build())
    response    <- ZIO.attemptBlocking(s3Client.headObject(headRequest))
  yield StoredBlob(
    name = s"${s3Path.bucket}/${s3Path.objectKey}",
    createdOn = Some(response.lastModified().getEpochSecond)
  )

  /** Saves the given text as a blob.
    */
  override def saveTextAsBlob(blobPath: S3StoragePath, data: String): Task[Unit] = for
    putObjectRequest <- ZIO.attempt(
      PutObjectRequest.builder().bucket(blobPath.bucket).key(blobPath.objectKey).contentType("text/plain").build()
    )
    response <- ZIO.attemptBlocking(s3Client.putObject(putObjectRequest, RequestBody.fromString(data)))
    _ <- ZIO.when(!response.sdkHttpResponse().isSuccessful)(
      zlog(
        "Failed to save a text file %s to S3, response: '%s'",
        blobPath.toHdfsPath,
        response.sdkHttpResponse().statusText().orElse("empty response")
      )
    )
  yield ()

  /** Removes the blob at the given path.
    */
  override def removeBlob(blobPath: S3StoragePath): Task[Unit] = for
    deleteObjectRequest <- ZIO.attempt(
      DeleteObjectRequest.builder().bucket(blobPath.bucket).key(blobPath.objectKey).build()
    )
    response <- ZIO.attemptBlocking(s3Client.deleteObject(deleteObjectRequest))
    _ <- ZIO.when(!response.sdkHttpResponse().isSuccessful)(
      zlog(
        "Failed to delete a file %s from S3, response: '%s'",
        blobPath.toHdfsPath,
        response.sdkHttpResponse().statusText().orElse("empty response")
      )
    )
  yield ()
