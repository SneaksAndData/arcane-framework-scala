package com.sneaksanddata.arcane.framework
package services.storage.base

import services.storage.models.base.{BlobPath, StoredBlob}

import com.sneaksanddata.arcane.framework.services.storage.models.azure.AdlsStoragePath
import zio.Task
import zio.stream.ZStream

import java.io.{BufferedReader, Reader}
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
   * @return A task containing the Reader instance. The reader returned by the function will be closed by the caller.
   */
  def streamBlobContent(blobPath: AdlsStoragePath): Task[Reader]

  /**
   * Streams the prefixes of the blobs at the given root prefix.
   * @param rootPrefix The root prefix.
   * @return The stream of the prefixes.
   */
  def streamPrefixes(rootPrefix: PathType): ZStream[Any, Throwable, StoredBlob]

  /**
   * Checks if the blob exists at the given path.
   * @param blobPath The path to the blob.
   * @return A future that will be completed with true if the blob exists, false otherwise.
   */
  def blobExists(blobPath: PathType): Task[Boolean]
