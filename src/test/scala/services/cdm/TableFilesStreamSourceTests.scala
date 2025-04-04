//package com.sneaksanddata.arcane.framework
//package services.cdm
//
//import models.settings.VersionedDataGraphBuilderSettings
//import services.storage.base.BlobStorageReader
//import services.storage.models.azure.AdlsStoragePath
//
//import com.sneaksanddata.arcane.framework.services.storage.models.base.StoredBlob
//import com.sneaksanddata.arcane.framework.services.synapse.SynapseLinkTableSettings
//import org.easymock.EasyMock.{anyString, replay, verify}
//import org.scalatest.flatspec.AsyncFlatSpec
//import org.scalatest.matchers.must.Matchers
//import org.scalatest.matchers.should.Matchers.should
//import org.scalatestplus.easymock.EasyMockSugar
//import zio.stream.ZStream
//import zio.{Runtime, Unsafe, ZIO}
//
//import java.io.{BufferedReader, StringReader}
//import java.time.Duration
//
//class TableFilesStreamSourceTests extends AsyncFlatSpec with Matchers with EasyMockSugar:
//  private val runtime = Runtime.default
//
//  private val settings = new VersionedDataGraphBuilderSettings {
//    override val lookBackInterval: Duration = Duration.ofHours(1)
//    override val changeCaptureInterval: Duration = Duration.ofSeconds(1)
//    override val changeCapturePeriod: Duration = Duration.ofMinutes(15)
//  }
//
//  it should "return a look back stream in the target table" in {
//    val reader = mock[BlobStorageReader[AdlsStoragePath]]
//    expecting {
//      reader.streamBlobContent(AdlsStoragePath("storageAccount","container","Changelog/changelog.info"))
//        .andReturn(ZIO.attempt(new BufferedReader(new StringReader("2021-09-01T00.00.00Z"))))
//        .once()
//      reader.streamPrefixes(AdlsStoragePath("storageAccount","container", anyString()))
//        .andReturn(ZStream.fromIterable(Seq[StoredBlob]()))
////        .andReturn(ZStream.repeat(blog))
//        .atLeastOnce()
//    }
//    val path = AdlsStoragePath("abfss://container@storageAccount.dfs.core.windows.net/").get
//    val tableSettings = SynapseLinkTableSettings("table", "abfss://container@storageAccount.dfs.core.windows.net/", None)
//    val stream = new TableFilesStreamSource(settings, reader, path, tableSettings).lookBackStream.runCollect
//
//    replay(reader)
//
//    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(stream)).map { _ =>
//      noException should be thrownBy verify(reader)
//    }
//  }
//
//  it should "return a change capture stream in the target table" in {
//    // Arrange
//    val reader = mock[BlobStorageReader[AdlsStoragePath]]
//
//    val rootBlob = StoredBlob(name = "2021-09-01T00.00.00Z", createdOn = None)
//    val tableBlob = StoredBlob(name = "2021-09-01T00.00.00Z/table/", createdOn = None)
//    val fileBlob = StoredBlob(name = "2021-09-01T00.00.00Z/table/1.csv", createdOn = None)
//    val modelBlobName = "2021-09-01T00.00.00Z/model.json"
//
//    val modelBlobContent =
//      """
//        |{
//        |  "name": "cdm",
//        |  "description": "cdm",
//        |  "version": "1.0",
//        |  "entities":
//        |  [
//        |    {
//        |      "$type":"LocalEntity",
//        |      "annotations":[],
//        |      "name": "table",
//        |      "description": "table",
//        |      "attributes":
//        |      [
//        |        {"name":"Id","dataType":"guid","maxLength":-1}
//        |      ]
//        |    }
//        |  ]
//        |}
//        |""".stripMargin
//
//    // This is an expected happy path call sequence for the MicrosoftSynapseLinkDataProvider
//    expecting {
//
//      // The changelog file is read 4 times, once for the initial read, and twice for the change capture stream
//      reader.streamBlobContent(AdlsStoragePath("storageAccount", "container", "Changelog/changelog.info"))
//        .andReturn(ZIO.attempt(new BufferedReader(new StringReader("2021-09-01T00.05.00Z"))))
//        .atLeastOnce()
//
//      // Iterate over timestamps, starting from the current time minus at least one hour.
//      reader.streamPrefixes(AdlsStoragePath("storageAccount","container", "2021-08-31T23"))
//        // We mock the stream to return the same blob twice since the stream drops the last element
//        .andReturn(ZStream.fromIterable(Seq(rootBlob, rootBlob)))
//        .anyTimes()
//
//      // Iterate over timestamps, starting from the current time minus at least one hour.
//      reader.streamPrefixes(AdlsStoragePath("storageAccount","container", "2021-09-01T00"))
//        // We mock the stream to return the same blob twice since the stream drops the last element
//        .andReturn(ZStream.fromIterable(Seq(rootBlob, rootBlob)))
//        .anyTimes()
//
//      // We simulate the stream of the table blob
//      reader.streamPrefixes(AdlsStoragePath("storageAccount","container", rootBlob.name))
//        .andReturn(ZStream.succeed(tableBlob))
//        .anyTimes()
//
//      // We simulate the stream of the file blob
//      reader.streamPrefixes(AdlsStoragePath("storageAccount","container", tableBlob.name))
//        .andReturn(ZStream.succeed(fileBlob))
//        .anyTimes()
//
//      // We simulate the stream of the schema file
//      // Since the stream is called twice, we must return the same content twice (once for each read)
//      reader.streamBlobContent(AdlsStoragePath("storageAccount","container", modelBlobName))
//        .andReturn(ZIO.succeed(new BufferedReader(new StringReader(modelBlobContent))))
//        .times(2)
//
//      // We must return true when checking if the blob exists since the stream checks if the model.json exists and
//      // filters out the prefixes that do not have it
//      reader.blobExists(AdlsStoragePath("storageAccount","container", anyString()))
//        .andReturn(ZIO.succeed(true))
//        .atLeastOnce()
//    }
//    replay(reader)
//    val path = AdlsStoragePath("abfss://container@storageAccount.dfs.core.windows.net/").get
//    val tableSettings = SynapseLinkTableSettings("table", "abfss://container@storageAccount.dfs.core.windows.net/", None)
//
//    // Act
//    val stream = new TableFilesStreamSource(settings, reader, path, tableSettings).changeCaptureStream.take(2).runCollect
//
//    // Assert
//    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(stream)).map { _ =>
//      noException should be thrownBy verify(reader)
//    }
//  }
