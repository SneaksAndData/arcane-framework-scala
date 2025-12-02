package com.sneaksanddata.arcane.framework
package tests.shared

import services.storage.models.s3.S3ClientSettings
import services.storage.services.s3.S3BlobStorageReader

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}

object S3StorageInfo:
  val bucket                     = "s3-blob-reader"
  val jsonBucket                 = "s3-blob-reader-json"
  val jsonBucketVariable         = "s3-blob-reader-json-variable"
  val jsonBucketNestedArray      = "s3-blob-reader-json-nested-array"
  val jsonBucketRootNestedArray  = "s3-blob-reader-json-root-nested-array"
  val jsonBucketRootNestedJArray = "s3-blob-reader-json-root-nested-jarray"
  val accessKeyId                = "minioadmin"
  val secretAccessKey            = "minioadmin"
  val endpoint                   = "http://localhost:9000"

  val storageReader = S3BlobStorageReader(
    StaticCredentialsProvider.create(AwsBasicCredentials.create(secretAccessKey, accessKeyId)),
    Some(
      S3ClientSettings(
        region = Some("us-east-1"),
        endpoint = Some(endpoint),
        pathStyleAccess = true,
        maxResultsPerPage = 5
      )
    )
  )
