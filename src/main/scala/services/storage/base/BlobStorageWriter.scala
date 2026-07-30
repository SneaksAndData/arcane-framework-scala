package com.sneaksanddata.arcane.framework
package services.storage.base

import services.storage.models.base.BlobPath

import zio.Task

import java.net.URL
import scala.concurrent.Future

/** A trait that defines the interface for writing to a blob storage.
  *
  * @tparam Path
  *   The type of the path to the blob.
  */
trait BlobStorageWriter[Path <: BlobPath]:
  /** Saves the given text as a blob.
    */
  def saveTextAsBlob(blobPath: Path, data: String): Task[Unit]

  /** Removes the blob at the given path.
    */
  def removeBlob(blobPath: Path): Task[Unit]

  /** Removes the blob at the given path.
    */
  def removeBlob(blobPath: String): Task[Unit]
