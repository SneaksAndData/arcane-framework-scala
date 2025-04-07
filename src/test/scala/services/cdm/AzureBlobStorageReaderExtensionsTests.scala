//package com.sneaksanddata.arcane.framework.tests
//package services.cdm
//
//import services.cdm.AzureBlobStorageReaderExtensions.*
//import services.storage.models.azure.AdlsStoragePath
//import services.storage.services.AzureBlobStorageReader
//
//import com.azure.storage.common.StorageSharedKeyCredential
//import com.sneaksanddata.arcane.framework.services.storage.base.BlobStorageReader
//import com.sneaksanddata.arcane.framework.services.storage.models.base.StoredBlob
//import org.easymock.EasyMock
//import org.easymock.EasyMock.{replay, verify}
//import org.scalatest.Inspectors
//import org.scalatest.flatspec.AsyncFlatSpec
//import org.scalatest.matchers.must.Matchers
//import org.scalatest.matchers.should.Matchers.should
//import org.scalatest.prop.TableDrivenPropertyChecks.forAll
//import org.scalatest.prop.Tables.Table
//import org.scalatestplus.easymock.EasyMockSugar
//import zio.stream.ZStream
//import zio.{Runtime, Unsafe}
//
//import java.time.format.DateTimeFormatter
//import java.time.{Duration, OffsetDateTime, ZoneOffset}
//import scala.math.Ordered.orderingToOrdered
//
//class AzureBlobStorageReaderExtensionsTests extends AsyncFlatSpec with Matchers with EasyMockSugar:
//  private val runtime = Runtime.default
//
//  private val endpoint = "http://localhost:10001/devstoreaccount1"
//  private val container = "cdm-e2e"
//  private val storageAccount = "devstoreaccount1"
//  private val accessKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
//  private val tableName = "dimensionattributelevelvalue"
//
//  private val credential = StorageSharedKeyCredential(storageAccount, accessKey)
//  private val storageReader = AzureBlobStorageReader(storageAccount, endpoint, credential)
//  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH.mm.ssX")
//

//  it should "be able read the schemas from the schema folder" in {
//    val path = AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/").get
//    val startDate = OffsetDateTime.now().minus(Duration.ofHours(12))
//
//    val stream = storageReader.streamTableContent(path, startDate, OffsetDateTime.now(), tableName).map(r => r.schemaProvider.getSchema).runCollect
//    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(stream)).map { result =>
//      // Check that all schemas are the same for this table
//      result forall(_ == result.head) should be(true)
//    }
//  }
//
//  it should "be able to filter the exact matches" in {
//    val path = AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/").get
//    val startDate = OffsetDateTime.now().minus(Duration.ofHours(12))
//
//    val tableName = "dimensionattributelevel"
//    val allPrefixes = storageReader.streamTableContent(path, startDate, OffsetDateTime.now(), tableName).runCollect
//
//    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(allPrefixes)).map { result =>
//      val prefixes = result.map(blob => blob.blob.name).toList
//
//      Inspectors.forAll(prefixes) { name =>
//        name should (include("dimensionattributelevel") and not include("dimensionattributelevelvalue"))
//      }
//    }
//  }
//
//  it should "be able to list all files belonging to a specific table in the storage container" in {
//    val path = AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/").get
//    val startDate = OffsetDateTime.now().minus(Duration.ofHours(12))
//    val tableName = "dimensionattributelevel"
//
//    val allPrefixes = storageReader.streamTableContent(path, startDate, OffsetDateTime.now(), tableName).runCollect
//
//    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(allPrefixes)).map { result =>
//      val prefixes = result.map(blob => blob.blob.name).toList
//
//      // Note that the last ROOT prefix is dropped and the size of the final list of CSVs in container is 21 instead of 24
//      prefixes should have size 21
//    }
//  }
//
