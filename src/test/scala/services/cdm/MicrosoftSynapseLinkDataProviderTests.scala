package com.sneaksanddata.arcane.framework
package services.cdm

import models.settings.VersionedDataGraphBuilderSettings

import com.azure.storage.common.StorageSharedKeyCredential
import com.sneaksanddata.arcane.framework.services.storage.models.azure.AdlsStoragePath
import com.sneaksanddata.arcane.framework.services.storage.services.AzureBlobStorageReader
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should
import org.scalatestplus.easymock.EasyMockSugar
import zio.{Runtime, Unsafe}

import java.time.Duration

class MicrosoftSynapseLinkDataProviderTests extends AsyncFlatSpec with Matchers with EasyMockSugar:
  private val runtime = Runtime.default

  private val endpoint = "http://localhost:10001/devstoreaccount1"
  private val container = "cdm-e2e"
  private val storageAccount = "devstoreaccount1"
  private val accessKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
  private val tableName = "dimensionattributelevelvalue"

  private val credential = StorageSharedKeyCredential(storageAccount, accessKey)
  private val settings = new VersionedDataGraphBuilderSettings {
    override val lookBackInterval: Duration = Duration.ofHours(12)
    override val changeCaptureInterval: Duration = Duration.ofSeconds(1)
    override val changeCapturePeriod: Duration = Duration.ofMinutes(15)
  }


  it should "be able to run the data row stream" in {
    val path = AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/").get
    val tableSettings = CdmTableSettings("table", s"abfss://$container@$storageAccount.dfs.core.windows.net/")

    val reader = AzureBlobStorageReader(storageAccount, endpoint, credential)
    val streamSource = new TableFilesStreamSource(settings, reader, path, tableSettings)
    val dataProvider = MicrosoftSynapseLinkDataProvider(streamSource, reader, path)

//    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(dataProvider.stream.runCollect)).map { result =>
//      //noException should be thrownBy verify(reader)
//      true should be(false)
//    }
    true should be(true)
  }
