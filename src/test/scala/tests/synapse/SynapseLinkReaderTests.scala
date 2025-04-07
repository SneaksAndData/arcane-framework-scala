package com.sneaksanddata.arcane.framework
package tests.synapse

import com.azure.storage.common.StorageSharedKeyCredential
import services.storage.models.azure.AdlsStoragePath
import services.storage.services.AzureBlobStorageReader
import services.synapse.base.SynapseLinkReader
import tests.synapse.SynapseAzureBlobReaderExtensionsTests.{container, storageAccount}

import zio.{Scope, ZIO}
import zio.test.*
import zio.test.TestAspect.timeout

import java.time.{Duration, OffsetDateTime}

object SynapseLinkReaderTests extends ZIOSpecDefault:
  private val endpoint = "http://localhost:10001/devstoreaccount1"
  private val container = "cdm-e2e"
  private val storageAccount = "devstoreaccount1"
  private val accessKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
  private val tableName = "dimensionattributelevelvalue"

  private val credential = StorageSharedKeyCredential(storageAccount, accessKey)
  private val storageReader = AzureBlobStorageReader(storageAccount, endpoint, credential)
  
  override def spec: Spec[TestEnvironment & Scope, Any] = suite("SynapseLinkReader") (
    test("streams changes belonging to the configured table") {
      for
        path <- ZIO.succeed(AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/").get)
        synapseLinkReader <- ZIO.succeed(SynapseLinkReader(storageReader, tableName, path))
        allRows <- synapseLinkReader.getChanges(OffsetDateTime.now().minus(Duration.ofHours(12))).map(_ => 1).runSum
        // expect 35 rows, since each file has 5 rows, total 8 files for this table and 1 file skipped as it is the latest one
      yield assertTrue(allRows == 5 * (8 - 1))
    }
  ) @@ timeout(zio.Duration.fromSeconds(10)) @@ TestAspect.withLiveClock
