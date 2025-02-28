package com.sneaksanddata.arcane.framework
package services.cdm

import services.cdm.AzureBlobStorageReaderExtensions.*
import services.storage.models.azure.AdlsStoragePath
import services.storage.services.AzureBlobStorageReader

import com.azure.storage.common.StorageSharedKeyCredential
import org.scalatest.Inspectors
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table
import zio.{Runtime, Unsafe}

import java.time.format.DateTimeFormatter
import java.time.{Duration, OffsetDateTime, ZoneOffset}

class AzureBlobStorageReaderExtensionsTests extends AsyncFlatSpec with Matchers:
  private val runtime = Runtime.default

  private val endpoint = "http://localhost:10001/devstoreaccount1"
  private val container = "cdm-e2e"
  private val storageAccount = "devstoreaccount1"
  private val accessKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
  private val tableName = "dimensionattributelevelvalue"

  private val credential = StorageSharedKeyCredential(storageAccount, accessKey)
  private val storageReader = AzureBlobStorageReader(storageAccount, endpoint, credential)
  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH.mm.ssX")

  /*
    * Test cases for getRootPrefixes method. These tast cases reliing on the following assumptions:
    * - The test date was created by the populate-cdm-container.py script no longer than 1 hour ago
    * - The test container was cleaned up before running the populate-cdm-container.py script
    * Changing this behavior is too expensive for the current scope.
    * This will be fixed in https://github.com/SneaksAndData/arcane-framework-scala/issues/44
    * If tests are failing, please ensure that cdm-e2e container is empty and run populate-cdm-container.py script
   */
  private val rootPrefixesTestCases = Table(
    ("StartDate", "ExpectedPrefixesCount"),

    // 12 hours ago, we should get all timestamp folders as a result.
    (OffsetDateTime.now().minus(Duration.ofHours(12)), 8),

    // 2 hours ago, we should get 2 folders as a result since folder with same hours created later than our start time.
    (OffsetDateTime.now(ZoneOffset.UTC).minus(Duration.ofHours(3)), 2),

    // 1 hour ago, we should not get anything as a result since folder with same hours created later than our start time.
    (OffsetDateTime.now(ZoneOffset.UTC).minus(Duration.ofHours(1)), 0),

    // Current time, we should get nothing
    (OffsetDateTime.now(), 0),
  )

  it should "be able read root prefixes starting from specific dates" in {
    forAll(rootPrefixesTestCases) { (startDate, expected) =>
      val path = AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/").get
      val stream = storageReader.getRootPrefixes(path, startDate).runCollect
      Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(stream)).map { result =>
        result.length should be(expected)
      }
    }
  }


  it should "be able read the schemas from the schema folder" in {
    val path = AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/").get
    val startDate = OffsetDateTime.now().minus(Duration.ofHours(12))

    val stream = storageReader.getRootPrefixes(path, startDate).enrichWithSchema(storageReader, path, tableName).runCollect
    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(stream)).map { result =>
      result.length should be(8)
    }
  }

  it should "be able to filter the exact matches" in {
    val path = AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/").get
    val startDate = OffsetDateTime.now().minus(Duration.ofHours(12))

    val allPrefixes = storageReader
      .getRootPrefixes(path, startDate)
      .enrichWithSchema(storageReader, path, tableName)
      .flatMap(seb => storageReader.streamPrefixes(path + seb.blob.name).addSchema(seb.schemaProvider))
      .filterByTableName("dimensionattributelevel")
      .runCollect

    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(allPrefixes)).map { result =>
      val prefixes = result.map(blob => blob.blob.name).toList
      
      Inspectors.forAll(prefixes) { name =>
        name should (include("dimensionattributelevel") and not include("dimensionattributelevelvalue"))
      }
    }
  }

  it should "be able list all files belonging to a specific table in the storage container" in {
    val path = AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/").get
    val startDate = OffsetDateTime.now().minus(Duration.ofHours(12))
    val tableName = "dimensionattributelevel"

    val allPrefixes = storageReader
      .getRootPrefixes(path, startDate)
      .enrichWithSchema(storageReader, path, tableName)
      .getFilesStream(storageReader, tableName, path)
      .runCollect

    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(allPrefixes)).map { result =>
      val prefixes = result.map(blob => blob.blob.name).toList
      prefixes should have size 24
    }
  }
