package com.sneaksanddata.arcane.framework
package services.storage.models.dynamodb

import models.serialization.S3RegionRW.*
import models.serialization.UriRW.*
import models.serialization.JavaDurationRW.*

import software.amazon.awssdk.regions.Region
import upickle.ReadWriter

import java.net.URI
import java.time.Duration

case class DynamodbClientSettings(
    // connection config
    region: Option[Region],
    tableName: String,
    endpoint: Option[URI],
    autoCreateTable: Boolean,
    // ingestion config
    usePathStyle: Boolean,
    maxResultsPerPage: Int,
    retryMaxAttempts: Int,
    retryBaseDelay: Duration,
    retryMaxDelay: Duration
) derives ReadWriter

object DynamodbClientSettings:
  def apply(
      region: Option[String] = Some("eu-central-1"),
      endpoint: Option[String] = None,
      pathStyleAccess: Boolean = false,
      maxResultsPerPage: Int = 1000,
      retryMaxAttempts: Int = 5,
      retryBaseDelay: Duration = Duration.ofMillis(100),
      retryMaxDelay: Duration = Duration.ofSeconds(1),
      tableName: String = "arcane-ingestion",
      autoCreateTable: Boolean = false
  ): DynamodbClientSettings = new DynamodbClientSettings(
    region = region.map(Region.of),
    tableName = tableName,
    endpoint = endpoint.map(URI.create),
    autoCreateTable = autoCreateTable,
    usePathStyle = pathStyleAccess,
    maxResultsPerPage = maxResultsPerPage,
    retryMaxAttempts = retryMaxAttempts,
    retryBaseDelay = retryBaseDelay,
    retryMaxDelay = retryMaxDelay
  )
