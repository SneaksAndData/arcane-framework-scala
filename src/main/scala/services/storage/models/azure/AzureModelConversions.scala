package com.sneaksanddata.arcane.framework
package services.storage.models.azure

import com.azure.storage.blob.models.BlobItem
import services.storage.models.base.StoredBlob

import scala.jdk.CollectionConverters.*
import scala.util.Try

object AzureModelConversions:
  given Conversion[BlobItem, StoredBlob] with
    override def apply(blobItem: BlobItem): StoredBlob =
      if Option(blobItem.getProperties).isDefined && Try(blobItem.getProperties.getCreationTime).isSuccess then
        StoredBlob(
          name = blobItem.getName,
          createdOn = Option(blobItem.getProperties.getCreationTime).map(_.toEpochSecond),
          metadata = Option(blobItem.getMetadata).map(_.asScala.toMap).getOrElse(Map()),
          contentHash = Option(blobItem.getProperties.getContentMd5).map(_.map(_.toChar).mkString),
          contentEncoding = Option(blobItem.getProperties.getContentEncoding),
          contentType = Option(blobItem.getProperties.getContentType),
          contentLength = Option(blobItem.getProperties.getContentLength),
          lastModified = Option(blobItem.getProperties.getLastModified.toEpochSecond)
        )
      else StoredBlob(
        name = blobItem.getName,
        createdOn = None,
        metadata = Map(),
        contentHash = None,
        contentEncoding = None,
        contentType = None,
        contentLength = None,
        lastModified = None
      )
