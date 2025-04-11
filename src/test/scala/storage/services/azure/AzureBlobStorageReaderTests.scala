package com.sneaksanddata.arcane.framework
package storage.services.azure

import services.storage.models.azure.AdlsStoragePath

import org.scalatest.matchers.should.Matchers.{should, shouldBe}
import zio.test.*
import zio.{Scope, Unsafe, ZIO}

import tests.shared.AzureStorageInfo.*

import zio.test.TestAspect.timeout

object AzureBlobStorageReaderTests extends ZIOSpecDefault:
  override def spec: Spec[TestEnvironment & Scope, Any] = suite("AzureBlobStorageReader")(
    Map(
      ("Changelog", false),
      ("Changelog/", false),
      ("Changelog/changelog.info", true),
      ("model.json", true),
      ("MissingFolder/", false),
      ("MissingFolder", false)
    ).map {
      case (blobName, expectedResult) => test(s"correctly check if a blob $blobName exists in a container") {
        for
          path <- ZIO.succeed(AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/$blobName").get)
          result <- storageReader.blobExists(path)
        yield assertTrue(result == expectedResult)
      }
    }.toSeq ++ Seq(
      test("lists files in a container") {
        for
          path <- ZIO.succeed(AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/").get)
          prefixes <- storageReader.streamPrefixes(path).runCount
        yield assertTrue(prefixes >= 8)
      },
    ) ++ Map(
      ("Changelog/changelog.info", true),
      ("MissingFolder/missing.txt", false),
      ("MissingFolder", false)
    ).map {
      case (blobName, expectedResult) => test(s"stream blob $blobName content from a container") {
        for
          path <- ZIO.succeed(AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/$blobName").get)
          content <- ZIO.acquireReleaseWith(storageReader.streamBlobContent(path))(reader => ZIO.succeed(reader.close())){ reader =>
            ZIO.attemptBlocking(Some(reader.readLine()))
          }.catchAllCause(_ => ZIO.succeed(Option.empty[String]))
        yield assertTrue(content.isDefined == expectedResult)
      }
    }
  ) @@ timeout(zio.Duration.fromSeconds(10)) @@ TestAspect.withLiveClock
