package com.sneaksanddata.arcane.framework
package services.blobsource

import models.app.PluginStreamContext
import models.settings.sources.blob.BlobSourceSettings
import services.storage.services.s3.S3BlobStorageService

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import zio.{ZIO, ZLayer}

/** ZLayer for S3Reader, to be used with S3-sourced BlobSource streams.
  */
object DefaultS3Service:
  private type SettingsExtractor = PluginStreamContext => BlobSourceSettings

  def getLayer(extractor: SettingsExtractor): ZLayer[PluginStreamContext, Nothing, S3BlobStorageService] = ZLayer {
    for
      context  <- ZIO.service[PluginStreamContext]
      settings <- ZIO.succeed(Some(extractor(context).s3ClientSettings))
    yield S3BlobStorageService(
      credentialsProvider = DefaultCredentialsProvider.create(),
      settings = settings
    )
  }
