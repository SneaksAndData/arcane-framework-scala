package com.sneaksanddata.arcane.framework
package services.blobsource

import models.settings.sources.blob.BlobSourceSettings
import services.storage.services.s3.S3BlobStorageReader

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import zio.{ZIO, ZLayer}

/** ZLayer for S3Reader, to be used with S3-sourced BlobSource streams.
  */
object DefaultS3Reader:
  val layer: ZLayer[BlobSourceSettings, Nothing, S3BlobStorageReader] = ZLayer {
    for
      config   <- ZIO.service[BlobSourceSettings]
      settings <- ZIO.succeed(Some(config.s3ClientSettings))
    yield S3BlobStorageReader(
      credentialsProvider = DefaultCredentialsProvider.create(),
      settings = settings
    )
  }
