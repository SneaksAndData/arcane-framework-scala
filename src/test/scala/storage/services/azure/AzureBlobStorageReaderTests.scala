package com.sneaksanddata.arcane.framework
package storage.services.azure

import services.storage.models.azure.AdlsStoragePath
import services.storage.services.AzureBlobStorageReader

import com.azure.storage.common.StorageSharedKeyCredential
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.{should, shouldBe}
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table
import zio.{Runtime, Unsafe}

import scala.util.Try

class AzureBlobStorageReaderTests extends AsyncFlatSpec with Matchers:
  private val runtime = Runtime.default

  private val endpoint = "http://localhost:10001/devstoreaccount1"
  private val container = "cdm-e2e"
  private val storageAccount = "devstoreaccount1"
  private val accessKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

  private val credential = StorageSharedKeyCredential(storageAccount, accessKey)
  private val storageReader = AzureBlobStorageReader(storageAccount, endpoint, credential)

  it should "be able to list files in a container" in {
    val path = AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/")
    val stream = storageReader.streamPrefixes(path.get).runCollect

    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(stream)).map { result =>
      result.size should be >= 8
    }
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
    forAll(blobsToList) { (blob, expected) =>
      val path = AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/$blob")
      Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(storageReader.blobExists(path.get))).map { result =>
        result shouldBe expected
      }
    }
  }

  private val blobToRead = Table(
    ("blob", "exptected_reuslt"),
    ("Changelog/changelog.info", true),
    ("MissingFolder/missing.txt", false),
    ("MissingFolder", false)
  )
  it should "be able to read data from container" in {
    forAll(blobsToList) { (blob, expected) =>
      val path = AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/$blob")
      Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(storageReader.streamBlobContent(path.get))).transform(result => {
        if (expected) {
          Try(result.get.readLine() should not be null)
        } else {
          Try(result.isFailure shouldBe true)
        }
      })
    }
  }
