//package com.sneaksanddata.arcane.framework
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
//  /*
//    * Test cases for getRootPrefixes method. These test cases relying on the following assumptions:
//    * - The test date was created by the populate-cdm-container.py script no longer than 1 hour ago
//    * - The test container was cleaned up before running the populate-cdm-container.py script
//    * Changing this behavior is too expensive for the current scope.
//    * This will be fixed in https://github.com/SneaksAndData/arcane-framework-scala/issues/44
//    * If tests are failing, please ensure that cdm-e2e container is empty and run populate-cdm-container.py script
//   */
//  private val getRootPrefixesTestCases = Table(
//    ("StartDate", "ExpectedPrefixesCount"),
//
//    // 12 hours ago, we should get all timestamp folders as a result.
//    (OffsetDateTime.now().minus(Duration.ofHours(12)), 8),
//
//    // 2 hours ago, we should get 2 folders as a result since folder with same hours created later than our start time.
//    (OffsetDateTime.now(ZoneOffset.UTC).minus(Duration.ofHours(3)), 2),
//
//    // 1 hour ago, we should not get anything as a result since folder with same hours created later than our start time.
//    (OffsetDateTime.now(ZoneOffset.UTC).minus(Duration.ofHours(1)), 0),
//
//    // Current time, we should get nothing
//    (OffsetDateTime.now(), 0),
//  )
//
//  it should "be able read root prefixes starting from specific dates" in {
//    forAll(getRootPrefixesTestCases) { (startDate, expected) =>
//      val path = AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/").get
//      val stream = storageReader.getRootPrefixes(path, startDate, OffsetDateTime.now()).runCollect
//      Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(stream)).map { result =>
//        result.length should be(expected)
//      }
//    }
//  }
//
//  /*
//    * The same as above, but these cases also validates the fact that getPrefixesFromDate method drops the last prefix
//   */
//  private val getPrefixesFromDateTestCases = Table(
//    ("StartDate", "ExpectedPrefixesCount"),
//
//    // 12 hours ago, we should get all timestamp folders as a result.
//    (OffsetDateTime.now().minus(Duration.ofHours(12)), 7),
//
//    // 2 hours ago, we should get 2 folders as a result since folder with same hours created later than our start time.
//    (OffsetDateTime.now(ZoneOffset.UTC).minus(Duration.ofHours(3)), 1),
//
//    // 1 hour ago, we should not get anything as a result since folder with same hours created later than our start time.
//    (OffsetDateTime.now(ZoneOffset.UTC).minus(Duration.ofHours(1)), 0),
//
//    // Current time, we should get nothing
//    (OffsetDateTime.now(), 0),
//  )
//
//  it should "be able read root prefixes starting from specific dates and drop last element" in {
//    forAll(getPrefixesFromDateTestCases) { (startDate, expected) =>
//      val path = AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/").get
//      val stream = storageReader.streamTableContent(path, startDate, OffsetDateTime.now(), tableName).runCollect
//      Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(stream)).map { result =>
//        result.length should be(expected)
//      }
//    }
//  }
//
//  it should "be able to read dates through hour change" in {
//    val path = AdlsStoragePath(s"abfss://container@storageAccount.dfs.core.windows.net/").get
//
//    val storageReader = mock[BlobStorageReader[AdlsStoragePath]]
//    val pastHour = List(
//        StoredBlob(name = "2021-08-01T23.40.10Z", createdOn = None),
//        StoredBlob(name = "2021-08-01T23.45.09Z", createdOn = None),
//        StoredBlob(name = "2021-08-01T23.50.10Z", createdOn = None),
//        StoredBlob(name = "2021-08-01T23.55.09Z", createdOn = None),
//    )
//
//    val currentHour = List(
//        StoredBlob(name = "2021-08-02T00.00.11Z", createdOn = None),
//        StoredBlob(name = "2021-08-02T00.05.11Z", createdOn = None),
//        StoredBlob(name = "2021-08-02T00.10.11Z", createdOn = None),
//    )
//
//    expecting {
//      storageReader.streamPrefixes(AdlsStoragePath("storageAccount", "container", "2021-08-01T23")).andReturn(ZStream.fromIterable(pastHour))
//      storageReader.streamPrefixes(AdlsStoragePath("storageAccount", "container", "2021-08-02T00")).andReturn(ZStream.fromIterable(currentHour))
//    }
//    replay(storageReader)
//
//    val lastBlobDate = OffsetDateTime.parse(currentHour.last.name, formatter)
//
//    val folders = storageReader.getRootPrefixes(path, lastBlobDate.minus(Duration.ofMinutes(30)), lastBlobDate).runCollect
//
//    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(folders)).map { result =>
//      verify(storageReader)
//
//      // Verify that the result contains all blobs from the past hour and the current hour
//      // The first blob from the past hour is not emitted since it is older than the start date - changeCapturePeriod (30 minutes)
//      result should contain theSameElementsInOrderAs (pastHour.tail ++ currentHour)
//    }
//  }
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
//  it should "be able to read latest commit date from ChangeLog folder" in {
//    val path = AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/").get
//
//    val changeLogEntry = storageReader.getLastCommitDate(path)
//
//    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(changeLogEntry)).map { result =>
//      // Validates that the latest commit date was parsed without exceptions
//      result <= OffsetDateTime.now() should be(true)
//    }
//  }
