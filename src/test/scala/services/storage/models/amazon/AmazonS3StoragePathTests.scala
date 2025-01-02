package com.sneaksanddata.arcane.framework
package services.storage.models.amazon

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table
import org.scalatest.matchers.should.Matchers._
import scala.util.{Failure, Success, Try}

//noinspection ScalaUnusedExpression
class AmazonS3StoragePathTests extends AnyFlatSpec with Matchers {

  "AmazonS3StoragePath" should "be able to parse correct path" in {
    val path = "s3a://bucket/key"
    val parsed = AmazonS3StoragePath(path)

    parsed should be (Success(AmazonS3StoragePath("bucket", "key")))
  }

  it should "have stable serialization and deserialization" in {
    val path = "s3a://bucket/key"
    val parsed = AmazonS3StoragePath(path)
    val serialized = AmazonS3StoragePath(parsed.get.toHdfsPath)

    parsed should be (Success(AmazonS3StoragePath(serialized.get.bucket, serialized.get.objectKey)))
  }

  it should "serialize and deserialize s3 paths in the same way" in {
    val path = "s3a://bucket/key"
    val parsed = AmazonS3StoragePath(path)
    val serialized = AmazonS3StoragePath(parsed.get.toHdfsPath)

    parsed.get.toHdfsPath should be (serialized.get.toHdfsPath)
  }

  it should "be able to jon paths" in {
    val path = "s3a://bucket/key"
    val parsed = AmazonS3StoragePath(path)
    val parsedJoined = parsed.get + "key2"

    parsedJoined should be (AmazonS3StoragePath("bucket", "key/key2"))
  }


  private val joinPathCases = Table(
    // First tuple defines column names
    ("original", "joined", "expected"),

    // Subsequent tuples define the data
    ("s3a://bucket-name/", "/folder1///folder2/file.txt", "s3a://bucket-name//folder1///folder2/file.txt"),
    ("s3a://bucket-name/", "folder1///folder2/file.txt", "s3a://bucket-name/folder1///folder2/file.txt"),
    ("s3a://bucket-name", "folder1///folder2/file.txt", "s3a://bucket-name/folder1///folder2/file.txt"),
    ("s3a://bucket-name", "/folder1///folder2/file.txt", "s3a://bucket-name//folder1///folder2/file.txt")
  )

  forAll (joinPathCases) { (orig: String, rest: String, expectedResult: String ) =>
    it should f"be able to remove extra slashes with values ($orig, $rest, $expectedResult)" in {
      val parsed = AmazonS3StoragePath(orig)
      val result = parsed.get + rest

      result.toHdfsPath should be (expectedResult)
    }
  }
}