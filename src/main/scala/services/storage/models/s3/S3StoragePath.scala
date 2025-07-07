package com.sneaksanddata.arcane.framework
package services.storage.models.s3

import services.storage.models.base.BlobPath

import scala.annotation.targetName
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

/** Represents a path to a blob in Amazon S3 storage.
  *
  * @param bucket
  *   The name of the bucket.
  * @param objectKey
  *   The key of the object in the bucket.
  */
final case class S3StoragePath(bucket: String, objectKey: String) extends BlobPath:

  /** Converts the path to a HDFS-style path.
    *
    * @return
    *   The path as a string.
    */
  override def toHdfsPath = s"s3a://$bucket/$objectKey"

  /** Joins the given key name to the current path.
    *
    * @param keyName
    *   The key name to join.
    * @return
    *   The new path.
    */
  @targetName("plus")
  def +(keyName: String): S3StoragePath =
    copy(objectKey = if (objectKey.isEmpty) keyName else s"$objectKey/$keyName")

/** Companion object for [[S3StoragePath]].
  */
object S3StoragePath {
  private val matchRegex: String = "s3a://([^/]+)/?(.*)"

  /** Creates an [[S3StoragePath]] from the given HDFS path.
    *
    * @param hdfsPath
    *   The HDFS path.
    * @return
    * The [[S3StoragePath]].
    */
  def apply(hdfsPath: String): Try[S3StoragePath] = matchRegex.r.findFirstMatchIn(hdfsPath) match {
    case Some(matched) => Success(new S3StoragePath(matched.group(1), matched.group(2).stripSuffix("/")))
    case None =>
      Failure(
        IllegalArgumentException(s"An AmazonS3StoragePath must be in the format s3a://bucket/path, but was: $hdfsPath")
      )
  }
}
