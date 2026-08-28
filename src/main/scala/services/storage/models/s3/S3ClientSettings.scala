package com.sneaksanddata.arcane.framework
package services.storage.models.s3

import models.serialization.S3RegionRW.*
import models.serialization.UriRW.*
import models.serialization.JavaDurationRW.*

import com.sneaksanddata.arcane.framework.models.settings.Mergeable
import software.amazon.awssdk.regions.Region
import upickle.ReadWriter

import java.net.URI
import java.time.Duration

case class S3ClientSettings(
    usePathStyle: Boolean,
    region: Option[Region],
    endpoint: Option[URI],
    maxResultsPerPage: Int,
    retryMaxAttempts: Int,
    retryBaseDelay: Duration,
    retryMaxDelay: Duration
) extends Mergeable[S3ClientSettings] derives ReadWriter:
  def merge(base: S3ClientSettings, overrides: S3ClientSettings): S3ClientSettings =
    S3ClientSettings(
      usePathStyle = overrides.usePathStyle,
      region = overrides.region.orElse(base.region),
      endpoint = overrides.endpoint.orElse(base.endpoint),
      maxResultsPerPage = overrides.maxResultsPerPage,
      retryMaxAttempts = overrides.retryMaxAttempts,
      retryBaseDelay = overrides.retryBaseDelay,
      retryMaxDelay = overrides.retryMaxDelay
    )

object S3ClientSettings:
  def apply(
      region: Option[String] = None,
      endpoint: Option[String] = None,
      pathStyleAccess: Boolean = false,
      maxResultsPerPage: Int = 1000,
      retryMaxAttempts: Int = 5,
      retryBaseDelay: Duration = Duration.ofMillis(100),
      retryMaxDelay: Duration = Duration.ofSeconds(1)
  ): S3ClientSettings = new S3ClientSettings(
    region = region.map(Region.of),
    endpoint = endpoint.map(URI.create),
    usePathStyle = pathStyleAccess,
    maxResultsPerPage = maxResultsPerPage,
    retryMaxAttempts = retryMaxAttempts,
    retryBaseDelay = retryBaseDelay,
    retryMaxDelay = retryMaxDelay
  )
