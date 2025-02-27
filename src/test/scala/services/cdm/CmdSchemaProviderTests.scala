package com.sneaksanddata.arcane.framework
package services.cdm

import services.storage.services.AzureBlobStorageReader

import com.azure.storage.common.StorageSharedKeyCredential
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should
import zio.{Runtime, Unsafe}

class CmdSchemaProviderTests extends AsyncFlatSpec with Matchers:
  private val runtime = Runtime.default

  private val endpoint = "http://localhost:10001/devstoreaccount1"
  private val container = "cdm-e2e"
  private val storageAccount = "devstoreaccount1"
  private val accessKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
  private val tableName = "dimensionattributelevelvalue"

  private val credential = StorageSharedKeyCredential(storageAccount, accessKey)
  private val storageReader = AzureBlobStorageReader(storageAccount, endpoint, credential)

  it should "be able to read schema from storage container" in {

    val path = s"abfss://$container@$storageAccount.dfs.core.windows.net/"
    val provider = CdmSchemaProvider(storageReader, path, tableName)

    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(provider.getSchema)).map { result =>
      result.length should be >= 1
    }
  }
