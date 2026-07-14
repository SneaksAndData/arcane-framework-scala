package com.sneaksanddata.arcane.framework
package services.storage.models.base

import scala.annotation.targetName

/** A trait that represents a path to a blob.
  */
trait BlobPath:

  /** Converts the path to a HDFS-style path.
    *
    * @return
    *   The path as a string.
    */
  def toHdfsPath: String

  def protocol: String

  @targetName("plus")
  def +(part: String): this.type 
