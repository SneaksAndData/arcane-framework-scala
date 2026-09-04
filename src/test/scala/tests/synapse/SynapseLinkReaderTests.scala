package com.sneaksanddata.arcane.framework
package tests.synapse

import models.schemas.{MergeKeyField, VersionField}
import models.settings.{AllFields, AllFieldsImpl, FieldSelectionRule, FieldSelectionRuleSettings}
import services.storage.models.azure.AdlsStoragePath
import services.synapse.base.SynapseLinkStreamingSource
import services.synapse.versioning.SynapseWatermark
import tests.shared.TestAzureStorageInfo.*

import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, ZIO}

import java.time.{Duration, OffsetDateTime}

object SynapseLinkReaderTests extends ZIOSpecDefault:
  private val tableName = "dimensionattributelevelvalue"
  private val allFieldsSelector = new FieldSelectionRuleSettings {

    /** The field selection rule to use.
      */
    override val rule: FieldSelectionRule = AllFieldsImpl(AllFields())

    /** The set of essential fields that must ALWAYS be included in the field selection rule. Fields from this list are
      * used in SQL queries and ALWAYS must be present in the result set. This list is provided by the Arcane streaming
      * plugin and should not be configurable.
      */
    override val essentialFields: Set[String] = Set.empty[String]
    override val isServerSide: Boolean        = false
    override type MergeableFrom = this.type
    override type MergeResult   = this.type
    override def merge(overrides: Option[MergeableFrom]): MergeResult = ???
  }

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("SynapseLinkStreamingSource")(
    test("streams changes belonging to the configured table") {
      for
        path <- ZIO.succeed(AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/").get)
        synapseLinkReader <- ZIO.succeed(
          new SynapseLinkStreamingSource(path, tableName, storageReader, allFieldsSelector, Seq.empty)
        )
        startFrom <- ZIO.succeed(OffsetDateTime.now().minus(Duration.ofHours(12)))
        allRows <- synapseLinkReader
          .getChanges(SynapseWatermark(version = "", timestamp = startFrom, prefix = ""))
          .flatMap(_._1)
          .map(_ => 1)
          .runSum // OffsetDateTime.now().minus(Duration.ofHours(12))
      // expect 30 rows, since each file has 5 rows
      // total 7 files for this table (first folder doesn't have a CSV/schema for this table)
      // 1 file skipped as it is the latest one
      // plus 1 record starting from folder 1 that contains 1 delete
      yield assertTrue(allRows == 5 * (7 - 1) + 1 * (7 - 1))
    },
    test("reads schema from a storage container and parses it successfully") {
      for
        path <- ZIO.succeed(AdlsStoragePath(s"abfss://$container@$storageAccount.dfs.core.windows.net/").get)
        synapseLinkReader <- ZIO.succeed(
          new SynapseLinkStreamingSource(path, tableName, storageReader, allFieldsSelector, Seq.empty)
        )
        schema <- synapseLinkReader.getSchema
      yield assertTrue(
        schema.size == 27,
        schema.exists(_.name == MergeKeyField.name),
        schema.exists(_.name == VersionField.name)
      )
    },
    test("fails on incorrect schema") {
      for
        path <- ZIO.succeed(
          AdlsStoragePath(s"abfss://$malformedSchemaContainer@$storageAccount.dfs.core.windows.net/").get
        )
        synapseLinkReader <- ZIO.succeed(
          new SynapseLinkStreamingSource(path, tableName, storageReader, allFieldsSelector, Seq.empty)
        )
        startFrom <- ZIO.succeed(OffsetDateTime.now().minus(Duration.ofHours(12)))
        exit <- synapseLinkReader
          .getChanges(SynapseWatermark(version = "", timestamp = startFrom, prefix = ""))
          .flatMap(_._1)
          .map(_ => 1)
          .runSum
          .exit
      yield assertTrue(exit.is(_.die).getMessage.startsWith("Unable to parse model.json file under location"))
    }
  ) @@ timeout(zio.Duration.fromSeconds(10)) @@ TestAspect.withLiveClock
