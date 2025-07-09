package com.sneaksanddata.arcane.framework
package tests.s3

import java.nio.file.Paths
import services.storage.models.s3.S3StoragePath
import tests.shared.S3StorageInfo.storageReader

import org.apache.iceberg.data.parquet.GenericParquetReaders
import org.apache.iceberg.parquet.Parquet
import org.apache.iceberg.types.Types.{NestedField, StringType}
import org.apache.iceberg.{Files, Schema, io}
import org.apache.parquet.hadoop.ParquetReader
import zio.test.{Spec, TestEnvironment, ZIOSpecDefault, assertTrue}
import zio.{Scope, ZIO}
import zio.stream.{ZSink, ZStream}

import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions

object S3BlobStorageReaderTests extends ZIOSpecDefault {
  val bucket = "s3-blob-reader"
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
        newSchema <- ZIO.succeed(
          Schema(
            List(
              NestedField.optional(0, "col0", StringType.get()),
              NestedField.optional(1, "col1", StringType.get())
            ).asJava
          )
        )
        path           <- ZIO.succeed(S3StoragePath(s"s3a://$bucket/0.parquet.gzip").get)
        downloadedFile <- storageReader.downloadBlob(path, "/tmp")
        data <- ZIO.attemptBlockingIO(
          Parquet
            .read(Files.localInput(downloadedFile))
            .project(newSchema)
            .createReaderFunc(schema =>
              GenericParquetReaders.buildReader(
                Schema(
                  List(
                    NestedField.optional(0, "col0", StringType.get()),
                    NestedField.optional(1, "col1", StringType.get())
                  ).asJava
                ),
                schema
              )
            )
            .build()
            .iterator()
        )
        rows <- ZStream.fromIterator(data.asScala).runCount
      yield assertTrue(rows == 100)
    }
  )
}
