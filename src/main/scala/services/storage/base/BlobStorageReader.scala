package com.sneaksanddata.arcane.framework
package services.storage.base

import services.storage.models.base.{BlobPath, StoredBlob}

import scala.concurrent.Future

/**
 * A trait that defines the interface for reading from a blob storage.
 *
 * @tparam PathType The type of the path to the blob.
 */
trait BlobStorageReader[PathType <: BlobPath]:
  /**
   * Gets the content of the blob at the given path.
   *
   * @param blobPath The path to the blob.
   * @param deserializer function to deserialize the content of the blob.
   * @tparam Result The type of the result.
   * @return The result of applying the function to the content of the blob.
   */
  def getBlobContent[Result](blobPath: PathType, deserializer: Array[Byte] => Result): Future[Result]
  
  def listBlobs(blobPath: PathType): LazyList[StoredBlob]
