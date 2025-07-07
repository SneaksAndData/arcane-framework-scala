package com.sneaksanddata.arcane.framework
package services.storage.models.s3

import software.amazon.awssdk.regions.Region

import java.net.URI

case class S3ClientSettings(
  usePathStyle: Boolean,
  region: Option[Region],
  endpoint: Option[URI],
  maxResultsPerPage: Int
)

object S3ClientSettings:
  def apply(
      region: Option[String] = None,
      endpoint: Option[String] = None,
      pathStyleAccess: Boolean = false,
        maxResultsPerPage: Int = 1000
  ): S3ClientSettings = new S3ClientSettings(
    region = region.map(Region.of),
    endpoint = endpoint.map(URI.create),
    usePathStyle = pathStyleAccess,
    maxResultsPerPage = maxResultsPerPage
  )