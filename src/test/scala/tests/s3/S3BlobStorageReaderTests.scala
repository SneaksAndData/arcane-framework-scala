package com.sneaksanddata.arcane.framework
package tests.s3

import java.nio.file.Paths
import services.storage.models.s3.S3StoragePath
import services.iceberg.interop.{ParquetScanner, given}
import services.iceberg.given_Conversion_MessageType_Schema
import tests.shared.S3StorageInfo.*

import org.apache.iceberg.data.parquet.GenericParquetReaders
import org.apache.iceberg.parquet.{Parquet, ParquetSchemaUtil}
import org.apache.iceberg.types.Types.{NestedField, StringType}
import org.apache.iceberg.{Files, Schema, io}
import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.hadoop.ParquetFileReader
import zio.test.{Spec, TestAspect, TestEnvironment, ZIOSpecDefault, assertTrue}
import zio.{Scope, ZIO}
import zio.stream.{ZSink, ZStream}
import zio.test.TestAspect.timeout

import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions

object S3BlobStorageReaderTests extends ZIOSpecDefault {
  override def spec: Spec[TestEnvironment & Scope, Any] = suite("S3BlobStorageReader")(
    test("blobExists returns true if a blob exists in a bucket") {
      for
        path   <- ZIO.succeed(S3StoragePath(s"s3a://$bucket/0.parquet.gzip").get)
        result <- storageReader.blobExists(path)
      yield assertTrue(result)
    },
    test("blobExists returns false and doesn't error if a blob does not exist in a bucket") {
      for
        path   <- ZIO.succeed(S3StoragePath(s"s3a://$bucket/0.parquet.zip").get)
        result <- storageReader.blobExists(path)
      yield assertTrue(!result)
    },
    // maxKeys 5 is applied during the test, ensuring we test pagination as well
    test("streamPrefixes returns a stream of correct length") {
      for
        path     <- ZIO.succeed(S3StoragePath(s"s3a://$bucket").get)
        prefixes <- storageReader.streamPrefixes(path).runCount
      yield assertTrue(prefixes == 50)
    },
    test("downloadBlob downloads a blob and it is readable afterwards") {
      for
        path           <- ZIO.succeed(S3StoragePath(s"s3a://$bucket/0.parquet.gzip").get)
        downloadedFile <- storageReader.downloadBlob(path, "/tmp")
        scanner        <- ZIO.succeed(ParquetScanner(downloadedFile))
        rows           <- scanner.getRows.runCollect
      yield assertTrue(rows.size == 100)
    }
  ) @@ timeout(zio.Duration.fromSeconds(10)) @@ TestAspect.withLiveClock
}
