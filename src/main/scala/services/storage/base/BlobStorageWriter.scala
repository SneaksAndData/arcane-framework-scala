package com.sneaksanddata.arcane.framework
package services.storage.base

import services.storage.models.base.BlobPath

import java.net.URL
import scala.concurrent.Future

/**
 * A trait that defines the interface for writing to a blob storage.
 *
 * @tparam Path The type of the path to the blob.
 */
trait BlobStorageWriter[Path <: BlobPath, Result]:
  /**
   * Saves the given bytes as a blob.
   *
   * @param blobPath The path to the blob.
   * @param data The bytes to save.
   * @return The result of the upload.
   */
  def saveBytesAsBlob(blobPath: Path, data: Array[Byte]): Future[Result]

  /**
   * Saves the given text as a blob.
   *
   * @param blobPath The path to the blob.
   * @param data The text to save.
   * @return The result of the upload.
   */
  def saveTextAsBlob(blobPath: Path, data: String): Future[Result]

  /**
   * Removes the blob at the given path.
   *
   * @param blobPath The path to the blob.
   * @param data The data to remove.
   */
  def removeBlob(blobPath: Path, data: String): Future[Result]

  /**
   * Gets the URI of the blob at the given path.
   *
   * @param blobPath The path to the blob.
   * @param data The data to get.
   * @return The URI of the blob.
   */
  def getBlobUri(blobPath: Path, data: String): Future[URL]
