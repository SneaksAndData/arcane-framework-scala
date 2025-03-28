package com.sneaksanddata.arcane.framework
package services.cdm

import services.storage.base.BlobStorageReader
import services.storage.models.azure.AdlsStoragePath
import services.storage.services.AzureBlobStorageReader

import com.azure.storage.common.StorageSharedKeyCredential
import com.sneaksanddata.arcane.framework.excpetions.StreamFailException
import org.easymock.EasyMock.{replay, verify}
import org.easymock.{EasyMock, IAnswer}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should
import org.scalatestplus.easymock.EasyMockSugar
import org.scalatestplus.easymock.EasyMockSugar.{expecting, mock}
import com.sneaksanddata.arcane.framework.models.given_CanAdd_ArcaneSchema
import com.sneaksanddata.arcane.framework.services.base.FrozenSchemaProvider.freeze
import zio.{Runtime, Unsafe, ZIO}

import java.io.{BufferedReader, StringReader}
import java.time.Duration

class CmdSchemaProviderTests extends AsyncFlatSpec with Matchers with EasyMockSugar:
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
    val provider = CdmSchemaProvider(storageReader, path, tableName, None)

    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(provider.getSchema)).map { result =>
      result.length should be >= 1
    }
  }

  "CdmSchemaProvider" should "read the schema every time" in {
    val path = s"abfss://$container@$storageAccount.dfs.core.windows.net/"
    val storageReaderMock = mock[BlobStorageReader[AdlsStoragePath]]
    expecting {
      storageReaderMock.streamBlobContent(AdlsStoragePath("devstoreaccount1","cdm-e2e",s"model.json")).andAnswer(() => {
        val arg = EasyMock.getCurrentArgument[AdlsStoragePath](0)
        storageReader.streamBlobContent(arg)
      })
      .times(5)
    }
    replay(storageReaderMock)
    val provider = CdmSchemaProvider(storageReaderMock, path, tableName, None)

    val task = provider.getSchema *> provider.getSchema *> provider.getSchema *> provider.getSchema *> provider.getSchema

    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(task)).map { result =>
        noException should be thrownBy verify(storageReaderMock)
    }
  }

  "CdmSchemaProvider" should "retry schema reads" in {
    val path = s"abfss://$container@$storageAccount.dfs.core.windows.net/"
    val storageReaderMock = mock[BlobStorageReader[AdlsStoragePath]]
    val modelBlobContentCorrect =
      """
        |{
        |  "name": "cdm",
        |  "description": "cdm",
        |  "version": "1.0",
        |  "entities":
        |  [
        |    {
        |      "$type":"LocalEntity",
        |      "annotations":[],
        |      "name": "table",
        |      "description": "table",
        |      "attributes":
        |      [
        |        {"name":"Id","dataType":"guid","maxLength":-1}
        |      ]
        |    }
        |  ]
        |}
        |""".stripMargin

    val modelBlobContentBroken =
        """
          |{
          |  "name": "cdm",
          |  "description": "cdm",
          |  "version": "1.0",
          |  "entities":
          |  [
          |    {
          |      "$type":"LocalEntity",
          |      "annotations":[],
          |      "name": "table",
          |      "description": "table",
          |      "attributes":
          |      [
          |        {"name":"Id","dataType":"guid","maxLength":-1}
          |""".stripMargin

    expecting {
      storageReaderMock
        .streamBlobContent(AdlsStoragePath("devstoreaccount1", "cdm-e2e", s"model.json"))
        .andReturn(ZIO.succeed(new BufferedReader(new StringReader(modelBlobContentBroken))))
        .andReturn(ZIO.succeed(new BufferedReader(new StringReader(modelBlobContentBroken))))
        .andReturn(ZIO.succeed(new BufferedReader(new StringReader(modelBlobContentCorrect))))

    }
    replay(storageReaderMock)
    val provider = CdmSchemaProvider(storageReaderMock, path, "table", None)

    val task = provider.getSchema

    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(task)).map { result =>
      verify(storageReaderMock)
      noException should be thrownBy verify(storageReaderMock)
    }
  }

  "CdmSchemaProvider" should "not retry schema reads if schema json is empty" in {
    val path = s"abfss://$container@$storageAccount.dfs.core.windows.net/"
    val storageReaderMock = mock[BlobStorageReader[AdlsStoragePath]]
    val modelBlobContentBroken =
      """
        | {"name":"cdm","description":"cdm","version":"1.0"}
        |""".stripMargin

    expecting {
      storageReaderMock
        .streamBlobContent(AdlsStoragePath("devstoreaccount1", "cdm-e2e", s"model.json"))
        .andReturn(ZIO.succeed(new BufferedReader(new StringReader(modelBlobContentBroken))))
        .anyTimes()
    }
    replay(storageReaderMock)
    val retrySettings = new RetrySettings:
      override val initialDelay: Duration = Duration.ofMillis(1)
      override val retryAttempts: Int = 3
      override val maxDuration: Duration = Duration.ofSeconds(3)

    val provider = CdmSchemaProvider(storageReaderMock, path, "table", Some(retrySettings))

    val task = provider.getSchema

    val future = Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(task))
    recoverToSucceededIf[StreamFailException](future)
  }

  "FrozenSchemaProvider" should "NOT read the schema every time" in {
    val path = s"abfss://$container@$storageAccount.dfs.core.windows.net/"
    val storageReaderMock = mock[BlobStorageReader[AdlsStoragePath]]
    expecting {
      storageReaderMock.streamBlobContent(AdlsStoragePath("devstoreaccount1", "cdm-e2e", s"model.json")).andAnswer(() => {
          val arg = EasyMock.getCurrentArgument[AdlsStoragePath](0)
          storageReader.streamBlobContent(arg)
        })
        .once()
    }
    replay(storageReaderMock)

    val task = CdmSchemaProvider(storageReaderMock, path, tableName, None).freeze.map(provider =>
      List.unfold(0, provider.getSchema, (i: Int) => if (i < 5) Some((i + 1, provider.getSchema)) else None)
    )


    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(task)).map { result =>
      noException should be thrownBy verify(storageReaderMock)
    }
  }
