package com.sneaksanddata.arcane.framework
package services.storage.services.azure

import logging.ZIOLogAnnotations.zlog
import models.settings.azure.AzureStorageConnectionSettings
import models.settings.azure.{SharedKeyImpl, DefaultImpl}
import services.storage.base.BlobStorageReader
import services.storage.models.azure.AdlsStoragePath
import services.storage.models.azure.AzureModelConversions.given
import services.storage.models.base.StoredBlob

import com.azure.core.credential.{AccessToken, TokenCredential, TokenRequestContext}
import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.storage.blob.models.{BlobListDetails, ListBlobsOptions}
import com.azure.storage.blob.{BlobClient, BlobContainerClient, BlobServiceClientBuilder}
import com.azure.storage.common.StorageSharedKeyCredential
import com.azure.storage.common.policy.{RequestRetryOptions, RetryPolicyType}
import reactor.core.publisher.Mono
import zio.stream.ZStream
import zio.{Schedule, Task, ZIO}

import java.io.{BufferedReader, InputStreamReader}
import java.time.Duration
import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions

/** Blob reader implementation for Azure. Relies on the default credential chain if no added credentials are provided.
  */
final class AzureBlobStorageReader(
    storageConnectionSettings: AzureStorageConnectionSettings
) extends BlobStorageReader[AdlsStoragePath]:

  private lazy val defaultCredential = new DefaultAzureCredentialBuilder().build()
  private lazy val serviceClient =
    val builder = storageConnectionSettings.credentialType match
      case SharedKeyImpl(sharedKey) =>
        new BlobServiceClientBuilder().credential(
          StorageSharedKeyCredential(storageConnectionSettings.accountName, sharedKey.accessKey)
        )
      case DefaultImpl(_) => new BlobServiceClientBuilder().credential(defaultCredential)

    builder
      .endpoint(
        storageConnectionSettings.endpoint
      )
      .retryOptions(
        RequestRetryOptions(
          RetryPolicyType.EXPONENTIAL,
          storageConnectionSettings.httpClient.httpMaxRetries,
          storageConnectionSettings.httpClient.httpRetryTimeout.toSeconds.toInt,
          storageConnectionSettings.httpClient.httpMinRetryDelay.toMillis,
          storageConnectionSettings.httpClient.httpMaxRetryDelay.toMillis,
          null
        )
      )
      .buildClient()

  private val defaultTimeout = Duration.ofSeconds(30)

  private def getBlobClient(blobPath: AdlsStoragePath): BlobClient =
    require(
      blobPath.accountName == storageConnectionSettings.accountName,
      s"Account name in the path `${blobPath.accountName}` does not match account name `${storageConnectionSettings.accountName}` initialized for this reader"
    )
    getBlobContainerClient(blobPath).getBlobClient(blobPath.blobPrefix)

  private def getBlobContainerClient(blobPath: AdlsStoragePath): BlobContainerClient =
    serviceClient.getBlobContainerClient(blobPath.container)

  private val retryPolicy = Schedule.exponential(Duration.ofSeconds(1)) && Schedule.recurs(10)

  /** @inheritdoc
    */
  override def streamBlobContent(blobPath: AdlsStoragePath): Task[BufferedReader] =
    for
      _      <- zlog("Downloading blob content from data file: %s", blobPath.toHdfsPath)
      client <- ZIO.succeed(getBlobClient(blobPath))
      stream <- ZIO.attemptBlocking {
        val stream = client.openInputStream()
        new BufferedReader(new InputStreamReader(stream))
      }
    yield stream

  override def streamPrefixes(rootPrefix: AdlsStoragePath): ZStream[Any, Throwable, StoredBlob] =
    val client = serviceClient.getBlobContainerClient(rootPrefix.container)
    val listOptions = new ListBlobsOptions()
      .setPrefix(rootPrefix.blobPrefix)
      .setMaxResultsPerPage(storageConnectionSettings.httpClient.maxResultsPerPage)
      .setDetails(
        BlobListDetails()
          .setRetrieveMetadata(false)
          .setRetrieveDeletedBlobs(false)
          .setRetrieveVersions(false)
      )

    val publisher = ZIO.attemptBlocking(
      client.listBlobsByHierarchy("/", listOptions, defaultTimeout).stream().toList.asScala.map(implicitly)
    )
    ZStream.fromIterableZIO(publisher) retry retryPolicy

  override def blobExists(blobPath: AdlsStoragePath): Task[Boolean] =
    ZIO
      .attemptBlocking(getBlobClient(blobPath).exists())
      .flatMap(result => ZIO.logDebug(s"Blob ${blobPath.toHdfsPath} exists: $result") *> ZIO.succeed(result))

  override def readBlobContent(blobPath: AdlsStoragePath): Task[String] = for
    client <- ZIO.attempt(getBlobClient(blobPath))
    _ <- zlog(
      "Reading file %s/%s from Azure Storage account %s",
      blobPath.container,
      blobPath.blobPrefix,
      blobPath.accountName
    )
    result <- ZIO.attemptBlockingIO(client.downloadContent().toBytes.map(_.toChar).mkString)
  yield result

  override def streamBlob(blobPath: AdlsStoragePath): ZStream[Any, Throwable, Byte] = ???

  override def downloadBlob(blobPath: AdlsStoragePath, localPath: String): Task[String] = ???

  override def downloadBlob(blobPath: String, localPath: String): Task[String] =
    downloadBlob(AdlsStoragePath(blobPath).get, localPath)

  override def downloadRandomBlob(rootPath: AdlsStoragePath, localPath: String): Task[Option[String]] = ???

object AzureBlobStorageReader:
  def apply(
      storageConnectionSettings: AzureStorageConnectionSettings
  ): AzureBlobStorageReader = new AzureBlobStorageReader(storageConnectionSettings)
