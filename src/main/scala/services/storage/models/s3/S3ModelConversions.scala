package com.sneaksanddata.arcane.framework
package services.storage.models.s3

import services.storage.models.base.StoredBlob

import software.amazon.awssdk.services.s3.model.S3Object

object S3ModelConversions:
  given Conversion[S3Object, StoredBlob] with
    override def apply(s3Obj: S3Object): StoredBlob =
      StoredBlob(
        metadata = Map(),
        contentHash = None,
        contentEncoding = None,
        contentType = None,
        contentLength = Some(s3Obj.size()),
        name = s3Obj.key(),
        lastModified = Some(s3Obj.lastModified().getEpochSecond),
        createdOn = Some(s3Obj.lastModified().getEpochSecond)
      )
