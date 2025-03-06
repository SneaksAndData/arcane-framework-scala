package com.sneaksanddata.arcane.framework
package services.cdm

import models.settings.VersionedDataGraphBuilderSettings
import services.storage.base.BlobStorageReader
import services.storage.models.azure.AdlsStoragePath

import org.easymock.EasyMock.{replay, verify}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should
import org.scalatestplus.easymock.EasyMockSugar
import zio.{Runtime, Unsafe, ZIO}

import java.io.{BufferedReader, StringReader}
import java.time.Duration

class TableFilesStreamSourceTests extends AsyncFlatSpec with Matchers with EasyMockSugar:
  private val runtime = Runtime.default

  private val settings = new VersionedDataGraphBuilderSettings {
    override val lookBackInterval: Duration = Duration.ofHours(1)
    override val changeCaptureInterval: Duration = Duration.ofSeconds(1)
    override val changeCapturePeriod: Duration = Duration.ofMinutes(15)
  }

  it should "return a look back stream in the target table" in {
    val reader = mock[BlobStorageReader[AdlsStoragePath]]
    expecting {
      reader.streamBlobContent(AdlsStoragePath("storageAccount","container","Changelog/changelog.info"))
        .andReturn(ZIO.attempt(new BufferedReader(new StringReader("2021-09-01T00.00.00Z"))))
        .once()
    }
    val path = AdlsStoragePath("abfss://container@storageAccount.dfs.core.windows.net/").get
    val tableSettings = CdmTableSettings("table", "abfss://container@storageAccount.dfs.core.windows.net/")
    val stream = new TableFilesStreamSource(settings, reader, path, tableSettings).lookBackStream.runCollect

    replay(reader)

    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(stream)).map { _ =>
      noException should be thrownBy verify(reader)
    }
  }

  it should "return a change capture stream in the target table" in {
    val reader = mock[BlobStorageReader[AdlsStoragePath]]
    expecting {
      reader.streamBlobContent(AdlsStoragePath("storageAccount", "container", "Changelog/changelog.info"))
        .andReturn(ZIO.attempt(new BufferedReader(new StringReader("2021-09-01T00.00.00Z"))))
        .times(3)
    }
    val path = AdlsStoragePath("abfss://container@storageAccount.dfs.core.windows.net/").get
    val tableSettings = CdmTableSettings("table", "abfss://container@storageAccount.dfs.core.windows.net/")
    val stream = new TableFilesStreamSource(settings, reader, path, tableSettings).changeCaptureStream.take(3).runCollect

    replay(reader)

    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(stream)).map { _ =>
      noException should be thrownBy verify(reader)
    }
  }
