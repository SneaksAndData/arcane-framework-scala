package com.sneaksanddata.arcane.framework
package services.storage.services.azure

import logging.ZIOLogAnnotations.zlog
import services.storage.base.BlobStorageReader
import services.storage.models.azure.AzureModelConversions.given
import services.storage.models.azure.{AdlsStoragePath, AzureBlobStorageReaderSettings}
import services.storage.models.base.{BlobPath, StoredBlob}

import com.azure.core.credential.TokenCredential
import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.storage.blob.models.{BlobListDetails, ListBlobsOptions}
import com.azure.storage.blob.{BlobClient, BlobContainerClient, BlobServiceClientBuilder}
import com.azure.storage.common.StorageSharedKeyCredential
import com.azure.storage.common.policy.{RequestRetryOptions, RetryPolicyType}
import zio.stream.ZStream
import zio.{Schedule, Task, ZIO}

import java.io.{BufferedReader, InputStreamReader}
import java.time.Duration
import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions

/** Blob reader implementation for Azure. Relies on the default credential chain if no added credentials are provided.
  * @param accountName
  *   Storage account name
  * @param tokenCredential
  *   Optional token credential provider
  * @param sharedKeyCredential
  *   Optional access key credential
  */
final class AzureBlobStorageReader(
    accountName: String,
    endpoint: Option[String],
    tokenCredential: Option[TokenCredential],
    sharedKeyCredential: Option[StorageSharedKeyCredential],
    settings: Option[AzureBlobStorageReaderSettings] = None
) extends BlobStorageReader[AdlsStoragePath]:

  private val serviceClientSettings  = settings.getOrElse(AzureBlobStorageReaderSettings())
  private lazy val defaultCredential = new DefaultAzureCredentialBuilder().build()
  private lazy val serviceClient =
    val builder = (tokenCredential, sharedKeyCredential) match
      case (Some(credential), _)    => new BlobServiceClientBuilder().credential(credential)
      case (None, Some(credential)) => new BlobServiceClientBuilder().credential(credential)
      case (None, None)             => new BlobServiceClientBuilder().credential(defaultCredential)

    builder
      .endpoint(endpoint.getOrElse("https://$accountName.blob.core.windows.net/"))
      .retryOptions(
        RequestRetryOptions(
          RetryPolicyType.EXPONENTIAL,
          serviceClientSettings.httpMaxRetries,
          serviceClientSettings.httpRetryTimeout.toSeconds.toInt,
          serviceClientSettings.httpMinRetryDelay.toMillis,
          serviceClientSettings.httpMaxRetryDelay.toMillis,
          null
        )
      )
      .buildClient()

  private val defaultTimeout = Duration.ofSeconds(30)

  private def getBlobClient(blobPath: AdlsStoragePath): BlobClient =
    require(
      blobPath.accountName == accountName,
      s"Account name in the path `${blobPath.accountName}` does not match account name `$accountName` initialized for this reader"
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
      .setMaxResultsPerPage(serviceClientSettings.maxResultsPerPage)
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

object AzureBlobStorageReader:
  /** Create AzureBlobStorageReader for the account using TokenCredential
    * @param accountName
    *   Storage account name
    * @param credential
    *   TokenCredential (accessToken provider)
    * @return
    *   AzureBlobStorageReader instance
    */
  def apply(accountName: String, credential: TokenCredential): AzureBlobStorageReader =
    new AzureBlobStorageReader(accountName, None, Some(credential), None)

  /** Create AzureBlobStorageReader for the account using StorageSharedKeyCredential
    *
    * @param accountName
    *   Storage account name
    * @param credential
    *   StorageSharedKeyCredential (account key)
    * @return
    *   AzureBlobStorageReader instance
    */
  def apply(accountName: String, credential: StorageSharedKeyCredential): AzureBlobStorageReader =
    new AzureBlobStorageReader(accountName, None, None, Some(credential))

  /** Create AzureBlobStorageReader for the account using StorageSharedKeyCredential and custom endpoint
    *
    * @param accountName
    *   Storage account name
    * @param endpoint
    *   Storage account endpoint
    * @param credential
    *   StorageSharedKeyCredential (account key)
    * @return
    *   AzureBlobStorageReader instance
    */
  def apply(accountName: String, endpoint: String, credential: StorageSharedKeyCredential): AzureBlobStorageReader =
    new AzureBlobStorageReader(accountName, Some(endpoint), None, Some(credential))

  /** Create AzureBlobStorageReader for the account using default credential chain
    *
    * @param accountName
    *   Storage account name
    * @return
    *   AzureBlobStorageReader instance
    */
  def apply(accountName: String): AzureBlobStorageReader =
    new AzureBlobStorageReader(accountName, None, None, None)
