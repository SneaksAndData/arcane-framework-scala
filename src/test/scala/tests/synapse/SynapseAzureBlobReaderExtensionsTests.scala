package com.sneaksanddata.arcane.framework
package tests.synapse

import com.azure.storage.common.StorageSharedKeyCredential
import services.storage.services.AzureBlobStorageReader
import services.synapse.SynapseAzureBlobReaderExtensions.*

import com.sneaksanddata.arcane.framework.services.storage.models.azure.AdlsStoragePath
import zio.stream.ZSink
import zio.{Scope, ZIO}
import zio.test.*
import zio.test.TestAspect.timeout

import java.time.{Duration, OffsetDateTime}
import java.time.format.DateTimeFormatter

object SynapseAzureBlobReaderExtensionsTests extends ZIOSpecDefault:
  private val endpoint = "http://localhost:10001/devstoreaccount1"
  private val container = "cdm-e2e"
  private val storageAccount = "devstoreaccount1"
  private val accessKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
  private val tableName = "dimensionattributelevelvalue"

  private val credential = StorageSharedKeyCredential(storageAccount, accessKey)
  private val storageReader = AzureBlobStorageReader(storageAccount, endpoint, credential)
  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH.mm.ssX")

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("SynapseAzureBlobReaderExtensions") (
    test("reads root prefixes correctly") {
      for 
        path <- ZIO.succeed(AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/").get)
        prefixes <- storageReader.getRootPrefixes(path, OffsetDateTime.now().minus(Duration.ofHours(12))).run(ZSink.collectAll)
      yield assertTrue(prefixes.size == 8)
    }
  )
