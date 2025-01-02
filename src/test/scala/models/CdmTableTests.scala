package com.sneaksanddata.arcane.framework
package models

import models.cdm.SimpleCdmModel
import services.storage.models.azure.AzureBlobStorageReader

import com.azure.storage.common.StorageSharedKeyCredential
import services.cdm.{CdmTable, CdmTableSettings}

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should
import org.scalatest.prop.Tables.Table
import org.scalatest.prop.TableDrivenPropertyChecks.*

import java.time.{OffsetDateTime, ZoneOffset}


class CdmTableTests extends AsyncFlatSpec with Matchers {
  private implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  private val integrationTestEndpoint = sys.env.get("ARCANE_FRAMEWORK__STORAGE_ENDPOINT")
  private val integrationTestContainer = sys.env.get("ARCANE_FRAMEWORK__STORAGE_CONTAINER")
  private val integrationTestAccount = sys.env.get("ARCANE_FRAMEWORK__STORAGE_ACCOUNT")
  private val integrationTestAccessKey = sys.env.get("ARCANE_FRAMEWORK__STORAGE_ACCESS_KEY")
  private val integrationTestTableName = sys.env.get("ARCANE_FRAMEWORK__CDM_TEST_TABLE")

  private val scanPeriods = Table(
    ("start", "end", "expected_rows"),
    (OffsetDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).minusHours(6), OffsetDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC), 0),
    (OffsetDateTime.of(2020, 1, 1, 1, 0, 0, 0, ZoneOffset.UTC).minusHours(1), OffsetDateTime.of(2020, 1, 1, 1, 0, 0, 0, ZoneOffset.UTC), 15),
    (OffsetDateTime.of(2020, 1, 1, 2, 0, 0, 0, ZoneOffset.UTC).minusHours(2), OffsetDateTime.of(2020, 1, 1, 2, 0, 0, 0, ZoneOffset.UTC), 20),
    (OffsetDateTime.of(2020, 1, 1, 3, 0, 0, 0, ZoneOffset.UTC).minusHours(3), OffsetDateTime.of(2020, 1, 1, 3, 0, 0, 0, ZoneOffset.UTC), 25),
    (OffsetDateTime.of(2020, 1, 2, 2, 0, 0, 0, ZoneOffset.UTC).minusDays(3), OffsetDateTime.of(2020, 1, 2, 2, 0, 0, 0, ZoneOffset.UTC), 30),
    (OffsetDateTime.of(2020, 2, 1, 1, 0, 0, 0, ZoneOffset.UTC).minusMonths(2), OffsetDateTime.of(2020, 2, 1, 2, 0, 0, 0, ZoneOffset.UTC), 35)
  )

  it should "read model schemas from Synapse Link blob storage" in {
    (integrationTestContainer, integrationTestAccount, integrationTestAccessKey, integrationTestEndpoint) match
      case (Some(container), Some(account), Some(key), Some(endpoint)) => SimpleCdmModel(s"abfss://$container@$account.dfs.core.windows.net/", AzureBlobStorageReader(account, endpoint, StorageSharedKeyCredential(account, key))).map { model =>
        model.entities.size should be > 0
      }
      case _ => cancel("Skipping test since it is not configured to run")
  }

  it should "read a CDM table from Synapse Link blob storage" in {
    forAll (scanPeriods) { (startDate, endDate, expectedRows) =>
      (integrationTestContainer, integrationTestAccount, integrationTestAccessKey, integrationTestTableName, integrationTestEndpoint) match
        case (Some(container), Some(account), Some(key), Some(tableName), Some(endpoint)) => SimpleCdmModel(s"abfss://$container@$account.dfs.core.windows.net/", AzureBlobStorageReader(account, endpoint, StorageSharedKeyCredential(account, key))).flatMap { model =>
          val entityToRead = model.entities.find(v => v.name == tableName).get
          CdmTable(CdmTableSettings(name = entityToRead.name, rootPath = s"abfss://$container@$account.dfs.core.windows.net/"), entityToRead, AzureBlobStorageReader(account, endpoint, StorageSharedKeyCredential(account, key)))
            .snapshot(Some(startDate), Some(endDate))
            .map { rows =>
              rows.foldLeft(0L) { (agg, _) => agg + 1 } should equal(expectedRows)
            }
        }
        case _ => cancel("Skipping test since it is not configured to run")
    }
  }
}
