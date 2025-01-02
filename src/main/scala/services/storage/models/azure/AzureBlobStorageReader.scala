package com.sneaksanddata.arcane.framework
package services.storage.models.azure

import services.storage.base.BlobStorageReader
import services.storage.models.azure.AzureModelConversions.given
import services.storage.models.base.StoredBlob

import com.azure.core.credential.TokenCredential
import com.azure.core.http.rest.PagedResponse
import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.storage.blob.models.ListBlobsOptions
import com.azure.storage.blob.{BlobClient, BlobContainerClient, BlobServiceClientBuilder}
import com.azure.storage.common.StorageSharedKeyCredential
import com.azure.storage.common.policy.{RequestRetryOptions, RetryPolicyType}

import java.time.Duration
import scala.annotation.tailrec
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions

/**
 * Blob reader implementation for Azure. Relies on the default credential chain if no added credentials are provided.
 * @param accountName Storage account name
 * @param tokenCredential Optional token credential provider
 * @param sharedKeyCredential Optional access key credential
 */
final class AzureBlobStorageReader(accountName: String, endpoint: Option[String], tokenCredential: Option[TokenCredential], sharedKeyCredential: Option[StorageSharedKeyCredential], settings: Option[AzureBlobStorageReaderSettings] = None) extends BlobStorageReader[AdlsStoragePath]:
  private val serviceClientSettings = settings.getOrElse(AzureBlobStorageReaderSettings()) 
  private lazy val defaultCredential = new DefaultAzureCredentialBuilder().build()
  private lazy val serviceClient =
    val builder = (tokenCredential, sharedKeyCredential) match
      case (Some(credential), _) => new BlobServiceClientBuilder().credential(credential)
      case (None, Some(credential)) => new BlobServiceClientBuilder().credential(credential)
      case (None, None) => new BlobServiceClientBuilder().credential(defaultCredential)

    builder
      .endpoint(endpoint.getOrElse("https://$accountName.blob.core.windows.net/"))
      .retryOptions(RequestRetryOptions(RetryPolicyType.EXPONENTIAL, serviceClientSettings.httpMaxRetries, serviceClientSettings.httpRetryTimeout.toSeconds.toInt, serviceClientSettings.httpMinRetryDelay.toMillis, serviceClientSettings.httpMaxRetryDelay.toMillis, null))
      .buildClient()

  private val defaultTimeout = Duration.ofSeconds(30)
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  private def getBlobClient(blobPath: AdlsStoragePath): BlobClient =
    require(blobPath.accountName == accountName, s"Account name in the path `${blobPath.accountName}` does not match account name `$accountName` initialized for this reader")
    getBlobContainerClient(blobPath).getBlobClient(blobPath.blobPrefix)

  private def getBlobContainerClient(blobPath: AdlsStoragePath): BlobContainerClient =
    serviceClient.getBlobContainerClient(blobPath.container)

  private val stringContentSerializer: Array[Byte] => String = _.map(_.toChar).mkString
  
  @tailrec
  private def getPage[ElementType, ResultElementType](pageToken: Option[String], result: Iterable[ResultElementType], pager: Option[String] => PagedResponse[ElementType])(implicit converter: ElementType => ResultElementType): Iterable[ResultElementType] =
    val page = pager(pageToken)
    val pageData = page.getValue.asScala.map(implicitly)
    val continuationToken = Option(page.getContinuationToken)

    if continuationToken.isEmpty then
      result ++ pageData
    else
      getPage(continuationToken, result ++ pageData, pager)

  /**
   *
   * @param blobPath The path to the blob.
   * @param deserializer function to deserialize the content of the blob. Deserializes all content as String if not implementation is provided
   * @tparam Result The type of the result.
   *  @return The result of applying the function to the content of the blob.
   */
  def getBlobContent[Result](blobPath: AdlsStoragePath, deserializer: Array[Byte] => Result = stringContentSerializer): Future[Result] =
    val client = getBlobClient(blobPath)
    Future(deserializer(client.downloadContent().toBytes))
    
  def listPrefixes(rootPrefix: AdlsStoragePath): LazyList[StoredBlob] =
    val client = getBlobContainerClient(rootPrefix)
    val listOptions = new ListBlobsOptions()
      .setPrefix(rootPrefix.blobPrefix)
      .setMaxResultsPerPage(serviceClientSettings.maxResultsPerPage)

    LazyList.from(getPage(
      None,
      List.empty[StoredBlob],
      token => client.listBlobsByHierarchy("/", listOptions, defaultTimeout).iterableByPage(token.orNull).iterator().next()
    ))

  def listBlobs(blobPath: AdlsStoragePath): LazyList[StoredBlob] =
    val client = getBlobContainerClient(blobPath)
    val listOptions = new ListBlobsOptions()
      .setPrefix(blobPath.blobPrefix)
      .setMaxResultsPerPage(serviceClientSettings.maxResultsPerPage)

    LazyList.from(getPage(
      None, 
      List.empty[StoredBlob], 
      token => client.listBlobs(listOptions, token.orNull, defaultTimeout).iterableByPage().iterator().next()
    ))

object AzureBlobStorageReader:
  /**
   * Create AzureBlobStorageReader for the account using TokenCredential
   * @param accountName Storage account name
   * @param credential TokenCredential (accessToken provider)
   * @return AzureBlobStorageReader instance
   */
  def apply(accountName: String, credential: TokenCredential): AzureBlobStorageReader = new AzureBlobStorageReader(accountName, None, Some(credential), None)

  /**
   * Create AzureBlobStorageReader for the account using StorageSharedKeyCredential
   *
   * @param accountName Storage account name
   * @param credential  StorageSharedKeyCredential (account key)
   * @return AzureBlobStorageReader instance
   */
  def apply(accountName: String, credential: StorageSharedKeyCredential): AzureBlobStorageReader = new AzureBlobStorageReader(accountName, None, None, Some(credential))

  /**
   * Create AzureBlobStorageReader for the account using StorageSharedKeyCredential and custom endpoint
   *
   * @param accountName Storage account name
   * @param endpoint Storage account endpoint                    
   * @param credential  StorageSharedKeyCredential (account key)
   * @return AzureBlobStorageReader instance
   */
  def apply(accountName: String, endpoint: String, credential: StorageSharedKeyCredential): AzureBlobStorageReader = new AzureBlobStorageReader(accountName, Some(endpoint), None, Some(credential))
  
  /**
   * Create AzureBlobStorageReader for the account using default credential chain
   *
   * @param accountName Storage account name
   * @return AzureBlobStorageReader instance
   */ 
  def apply(accountName: String): AzureBlobStorageReader = new AzureBlobStorageReader(accountName, None, None, None)
