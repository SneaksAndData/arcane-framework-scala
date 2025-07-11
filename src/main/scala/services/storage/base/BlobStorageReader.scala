package com.sneaksanddata.arcane.framework
package services.storage.base

import services.storage.models.base.{BlobPath, StoredBlob}

import com.sneaksanddata.arcane.framework.services.storage.models.azure.AdlsStoragePath
import zio.Task
import zio.stream.ZStream

import java.io.{BufferedReader, ByteArrayInputStream, Reader}
import scala.concurrent.Future

/** A trait that defines the interface for reading from a blob storage.
  *
  * @tparam PathType
  *   The type of the path to the blob.
  */
trait BlobStorageReader[PathType <: BlobPath]:
  /** Streams the content of the blob (as text) at the given path.
    *
    * @param blobPath
    *   The path to the blob.
    * @return
    *   A task containing a BufferedReader instance. The reader returned by the function should be closed by the caller.
    */
  def streamBlobContent(blobPath: PathType): Task[BufferedReader]

  /** Streams bytes from a blob at a given path.
    *
    * @param blobPath
    *   The path to the blob.
    * @return
    *   A task containing a ByteArrayInputStream instance. The reader returned by the function should be closed by the
    *   caller.
    */
  def streamBlob(blobPath: PathType): ZStream[Any, Throwable, Byte]

  /** Downloads a blob at a given path to a temporary folder
    *
    * @param blobPath
    *   The path to the blob as PathType
    * @return
    *   A path to the downloaded blob.
    */
  def downloadBlob(blobPath: PathType, localPath: String): Task[String]

  /** Downloads a blob at a given path to a temporary folder
    *
    * @param blobPath
    *   The path to the blob as String
    * @return
    *   A path to the downloaded blob.
    */
  def downloadBlob(blobPath: String, localPath: String): Task[String]

  /** Downloads a random blob found at a given path to a temporary folder
    *
    * @param rootPath
    *   The path containing one or more blob.
    * @return
    *   A path to the downloaded blob.
    */
  def downloadRandomBlob(rootPath: PathType, localPath: String): Task[String]

  /** Reads blob content as a string
    * @param blobPath
    *   Path to blob
    * @return
    */
  def readBlobContent(blobPath: PathType): Task[String]

  /** Streams the prefixes of the blobs at the given root prefix.
    * @param rootPrefix
    *   The root prefix.
    * @return
    *   The stream of the prefixes.
    */
  def streamPrefixes(rootPrefix: PathType): ZStream[Any, Throwable, StoredBlob]

  /** Checks if the blob exists at the given path.
    * @param blobPath
    *   The path to the blob.
    * @return
    *   A future that will be completed with true if the blob exists, false otherwise.
    */
  def blobExists(blobPath: PathType): Task[Boolean]
