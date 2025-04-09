package com.sneaksanddata.arcane.framework
package tests.synapse

import com.azure.storage.common.StorageSharedKeyCredential
import services.storage.services.AzureBlobStorageReader
import services.synapse.SynapseAzureBlobReaderExtensions.*

import services.storage.models.azure.AdlsStoragePath
import zio.stream.ZSink
import zio.{Scope, ZIO}
import zio.test.*
import zio.test.TestAspect.timeout

import java.time.{Duration, OffsetDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.collection.immutable

import tests.synapse.SynapseLinkStorageInfo._

object SynapseAzureBlobReaderExtensionsTests extends ZIOSpecDefault:
  /**
    * Test cases for getRootPrefixes method. These test cases relying on the following assumptions:
    * - The test date was created by the populate-cdm-container.py script no longer than 1 hour ago
    * - The test container was cleaned up before running the populate-cdm-container.py script
    * Changing this behavior is too expensive for the current scope.
    * This will be fixed in https://github.com/SneaksAndData/arcane-framework-scala/issues/44
    * If tests are failing, please ensure that cdm-e2e container is empty and run populate-cdm-container.py script
   **/
  private def rootPrefixesTests: immutable.Iterable[Spec[Any, Throwable]] =
    val testCases = Map(
      // 12 hours ago, we should get all timestamp folders but the latest one as a result.
      OffsetDateTime.now().minus(Duration.ofHours(12)) -> 7,

      // 3 hours ago, we should get 1 folder as a result since folder with same hours created later than our start time.
      OffsetDateTime.now(ZoneOffset.UTC).minus(Duration.ofHours(3)) -> 1,

      // 1 hour ago, we should not get anything as a result since folder with same hours created later than our start time.
      OffsetDateTime.now(ZoneOffset.UTC).minus(Duration.ofHours(1)) -> 0,

      // Current time, we should get nothing
      OffsetDateTime.now() -> 0,
    )

    testCases.map {
      case (startDate, expectedCount) => test("reads root prefixes correctly") {
        for
          path <- ZIO.succeed(AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/").get)
          prefixes <- storageReader.getRootPrefixes(path, startDate).run(ZSink.collectAll)
        yield assertTrue(prefixes.size == expectedCount)
      }
    }

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("SynapseAzureBlobReaderExtensions") (
    rootPrefixesTests
  ) @@ timeout(zio.Duration.fromSeconds(10)) @@ TestAspect.withLiveClock
