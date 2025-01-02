package com.sneaksanddata.arcane.framework
package services.storage.models.azure

import services.storage.models.base.BlobPath

import scala.annotation.targetName
import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex

final case class AdlsStoragePath(accountName: String, container: String, blobPrefix: String) extends BlobPath:
  def toHdfsPath: String = s"abfss://$container@$accountName.dfs.core.windows.net/$blobPrefix"

  /**
   * Joins the given key name to the current path.
   *
   * @param part Blob prefix part to join
   * @return The new path.
   */
  @targetName("plus")
  def +(part: String): AdlsStoragePath = copy(blobPrefix = if (blobPrefix.isEmpty) part else s"${blobPrefix.stripSuffix("/")}/$part")
  
object AdlsStoragePath:
  private val matchRegex: String = "^abfss:\\/\\/([^@]+)@([^\\.]+)\\.dfs\\.core\\.windows\\.net\\/(.*)$"
  
  def apply(hdfsPath: String): Try[AdlsStoragePath] = matchRegex.r.findFirstMatchIn(hdfsPath) match {
      case Some(matched) => Success(new AdlsStoragePath(matched.group(2), matched.group(1), matched.group(3).stripSuffix("/")))
      case None => Failure(IllegalArgumentException(s"An AdlsStoragePath must be in the format abfss://container@account.dfs.core.windows.net/path/to/file, but was: $hdfsPath"))
    }
  