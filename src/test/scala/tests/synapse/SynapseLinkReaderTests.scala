package com.sneaksanddata.arcane.framework
package tests.synapse

import services.storage.models.azure.AdlsStoragePath
import services.synapse.base.SynapseLinkReader
import tests.shared.AzureStorageInfo.*

import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, ZIO}

import java.time.{Duration, OffsetDateTime}

object SynapseLinkReaderTests extends ZIOSpecDefault:
  private val tableName = "dimensionattributelevelvalue"

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("SynapseLinkReader") (
    test("streams changes belonging to the configured table") {
      for
        path <- ZIO.succeed(AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/").get)
        synapseLinkReader <- ZIO.succeed(SynapseLinkReader(storageReader, tableName, path))
        allRows <- synapseLinkReader.getChanges(OffsetDateTime.now().minus(Duration.ofHours(12))).map(_ => 1).runSum
        // expect 30 rows, since each file has 5 rows
        // total 7 files for this table (first folder doesn't have a CSV/schema for this table) 
        // 1 file skipped as it is the latest one
        // plus 1 record starting from folder 1 that contains 1 delete
      yield assertTrue(allRows == 5 * (7 - 1) + 1 * (7 - 1))
    },

    test("reads schema from a storage container and parses it successfully") {
      for
        path <- ZIO.succeed(AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/").get)
        synapseLinkReader <- ZIO.succeed(SynapseLinkReader(storageReader, tableName, path))
        schema <- synapseLinkReader.getSchema
      // 25 fields plus ARCANE_MERGE_KEY  
      yield assertTrue(schema.size == 26)
    },

    test("fails on incorrect schema") {
      for
        path <- ZIO.succeed(AdlsStoragePath(s"abfss://$malformedSchemaContainer@$storageAccount.dfs.core.windows.net/").get)
        synapseLinkReader <- ZIO.succeed(SynapseLinkReader(storageReader, tableName, path))
        exit <- synapseLinkReader.getChanges(OffsetDateTime.now().minus(Duration.ofHours(12))).map(_ => 1).runSum.exit
      yield assertTrue(exit.is(_.die).getMessage.startsWith("Unable to parse model.json file under location"))
    }
  ) @@ timeout(zio.Duration.fromSeconds(10)) @@ TestAspect.withLiveClock
