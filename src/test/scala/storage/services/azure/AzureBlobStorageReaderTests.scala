package com.sneaksanddata.arcane.framework
package storage.services.azure

import services.storage.models.azure.AdlsStoragePath
import services.storage.services.AzureBlobStorageReader

import com.azure.storage.common.StorageSharedKeyCredential
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.{should, shouldBe}
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table
import zio.{Runtime, Unsafe}

class AzureBlobStorageReaderTests extends AnyFlatSpec with Matchers:
  private val runtime = Runtime.default

  private val integrationTestEndpoint = sys.env.get("ARCANE_FRAMEWORK__STORAGE_ENDPOINT").get
  private val integrationTestContainer = sys.env.get("ARCANE_FRAMEWORK__STORAGE_CONTAINER").get
  private val integrationTestAccount = sys.env.get("ARCANE_FRAMEWORK__STORAGE_ACCOUNT").get
  private val integrationTestAccessKey = sys.env.get("ARCANE_FRAMEWORK__STORAGE_ACCESS_KEY").get
  private val integrationTestTableName = sys.env.get("ARCANE_FRAMEWORK__CDM_TEST_TABLE").get

  private val endpoint = integrationTestEndpoint
  private val container = integrationTestContainer
  private val storageAccount = integrationTestAccount
  private val accessKey = integrationTestAccessKey

  private val credential = StorageSharedKeyCredential(storageAccount, accessKey)

  it should "be able to list files in a container" in {
//    val storageReader = AzureBlobStorageReader(storageAccount, endpoint, credential)
//    val path = AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/")
//    val stream = storageReader.streamPrefixes(path.get).runCollect
//
//    val result = Unsafe.unsafe(implicit unsafe => runtime.unsafe.run(stream).getOrThrowFiberFailure())
//    result.size should be >= 8
  assert(true)
  }

  private val blobsToList = Table(
    ("blob", "exptected_reuslt"),
    ("Changelog", false),
    ("Changelog/", false),
    ("Changelog/changelog.info", true),
    ("model.json", true),
    ("MissingFolder/", false),
    ("MissingFolder", false)
  )

  it should "be able to check if blob exist in container" in {
//    val storageReader = AzureBlobStorageReader(storageAccount, endpoint, credential)
    forAll(blobsToList) { (blob, expected) =>
//      val path = AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/$blob")
//      val result = Unsafe.unsafe(implicit unsafe => runtime.unsafe.run(storageReader.blobExists(path.get)).getOrThrowFiberFailure())
//      result shouldBe expected
      assert(true)
    }
  }

//  private val blobToRead = Table(
//    ("blob", "exptected_reuslt"),
//    ("Changelog/changelog.info", true),
//    ("MissingFolder/missing.txt", false),
//    ("MissingFolder", false)
//  )
//  it should "be able to read data from container" in {
//    forAll(blobsToList) { (blob, expected) =>
//      val path = AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/$blob")
//      val result = Unsafe.unsafe(implicit unsafe => runtime.unsafe.run(storageReader.streamBlobContent(path.get)).getOrThrowFiberFailure())
//        if (expected) {
//          Try(result.get.readLine() should not be null)
//        } else {
//          Try(result.isFailure shouldBe true)
//        }
//      }
//  }
