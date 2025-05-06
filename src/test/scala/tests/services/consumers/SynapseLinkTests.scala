package com.sneaksanddata.arcane.framework
package tests.services.consumers

import models.ArcaneType.{LongType, StringType}
import models.batches.*
import models.{Field, MergeKeyField}
import tests.shared.{CustomTablePropertiesSettings, TestTablePropertiesSettings}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table

import scala.io.Source
import scala.util.Using

class SynapseLinkTests extends AnyFlatSpec with Matchers:

  it should "generate a valid overwrite query" in {
    val query = SynapseLinkBackfillQuery("test.table_a",
      """SELECT * FROM (
        | SELECT * FROM test.staged_a ORDER BY ROW_NUMBER() OVER (PARTITION BY ARCANE_MERGE_KEY ORDER BY versionnumber DESC) FETCH FIRST 1 ROWS WITH TIES
        |) WHERE IsDelete = false""".stripMargin, TestTablePropertiesSettings)
    val expected = Using(Source.fromURL(getClass.getResource("/generate_an_overwrite_query_synapse_link.sql"))) {
      _.getLines().mkString("\n")
    }.get
    query.query should equal(expected)
  }

  it should "generate a valid merge query" in {
    val query = SynapseLinkMergeQuery(
      "test.table_a",
      """SELECT * FROM (
        | SELECT * FROM test.staged_a ORDER BY ROW_NUMBER() OVER (PARTITION BY ARCANE_MERGE_KEY ORDER BY versionnumber DESC) FETCH FIRST 1 ROWS WITH TIES
        |)""".stripMargin,
      Seq(),
      "ARCANE_MERGE_KEY",
      Seq("ARCANE_MERGE_KEY", "colA", "colB", "Id", "versionnumber")
    )

    val expected = Using(Source.fromURL(getClass.getResource("/generate_a_valid_merge_query_synapse_link.sql"))) {
      _.getLines().mkString("\n")
    }.get
    query.query should equal(expected)
  }

  it should "generate a valid merge with partitions" in {
    val query = SynapseLinkMergeQuery(
      "test.table_a",
      """SELECT * FROM (
        | SELECT * FROM test.staged_a ORDER BY ROW_NUMBER() OVER (PARTITION BY ARCANE_MERGE_KEY ORDER BY versionnumber DESC) FETCH FIRST 1 ROWS WITH TIES
        |)""".stripMargin,
      Seq("colA"),
      "ARCANE_MERGE_KEY",
      Seq("ARCANE_MERGE_KEY", "colA", "colB", "Id", "versionnumber")
    )

    val expected = Using(Source.fromURL(getClass.getResource("/generate_a_valid_merge_query_with_partitions_synapse_link.sql"))) {
      _.getLines().mkString("\n")
    }.get

    query.query should equal(expected)
  }

  "SynapseLinkBackfillOverwriteBatch" should "generate a valid backfill overwrite batch" in {
    val batch = SynapseLinkBackfillOverwriteBatch("test.staged_a", Seq(
      MergeKeyField,
      Field(
        name = "colA",
        fieldType = StringType
      ),
      Field(
        name = "colB",
        fieldType = StringType
      ),
      Field(
        name = "versionnumber",
        fieldType = LongType
      ),
      Field(
        name = "Id",
        fieldType = StringType
      )
    ), "test.table_a",
      TestTablePropertiesSettings)

    val expected = Using(Source.fromURL(getClass.getResource("/generate_a_valid_synapse_link_backfill_overwrite_query.sql"))) {
      _.getLines().mkString("\n")
    }.get

    batch.batchQuery.query should equal(expected)
  }
  
  "SynapseLinkBackfillMergeBatch" should "generate a valid backfill merge batch" in {
    val batch = SynapseLinkBackfillMergeBatch("test.staged_a", Seq(
      MergeKeyField,
      Field(
        name = "colA",
        fieldType = StringType
      ),
      Field(
        name = "colB",
        fieldType = StringType
      ),
      Field(
        name = "Id",
        fieldType = StringType
      ),
      Field(
        name = "versionnumber",
        fieldType = LongType
      )
    ), "test.table_a",
      CustomTablePropertiesSettings(Seq("bucket(colA, 32)", "year(colB)"))
    )

    val expected = Using(Source.fromURL(getClass.getResource("/generate_a_valid_synapse_link_backfill_merge_query.sql"))) {
      _.getLines().mkString("\n")
    }.get

    batch.batchQuery.query should equal(expected)
  }

  "SynapseLinkMergeBatch" should "generate a valid versioned batch" in {
    val batch = SynapseLinkMergeBatch("test.staged_a", Seq(
      MergeKeyField,
      Field(
        name = "colA",
        fieldType = StringType
      ),
      Field(
        name = "colB",
        fieldType = StringType
      ),
      Field(
        name = "Id",
        fieldType = StringType
      ),
      Field(
        name = "versionnumber",
        fieldType = LongType
      )
    ),
      "test.table_a",
      CustomTablePropertiesSettings(Seq("bucket(colA, 32)", "year(colB)"))
    )

    val expected = Using(Source.fromURL(getClass.getResource("/generate_a_valid_synapse_link_merge_query_with_partitions.sql"))) {
      _.getLines().mkString("\n")
    }.get

    batch.batchQuery.query should equal(expected)
  }

  private val mergeKeyStatements = Table(
    ("tablePropertiesSettings", "expectedResult"),
    (Seq("bucket(ARCANE_MERGE_KEY, 32)"), "filter_out_single_arcane_merge_key_from_merge_match_synapse_link"),
    (Seq("bucket(ARCANE_MERGE_KEY, 32)", "bucket(colA, 32)", "year(colB)"), "filter_out_arcane_merge_key_from_merge_match_synapse_link")
  )

  "SynapseLinkMergeBatch" should "filter out arcane merge key from merge match" in {
    val batchSchema = Seq(
      MergeKeyField,
      Field(
        name = "colA",
        fieldType = StringType
      ),
      Field(
        name = "colB",
        fieldType = StringType
      ),
      Field(
        name = "Id",
        fieldType = StringType
      ),
      Field(
        name = "versionnumber",
        fieldType = LongType
      )
    )
    forAll(mergeKeyStatements) { (partitionSpec, expectation) =>
      val tablePropertiesSettings = CustomTablePropertiesSettings(partitionSpec)
      val batch = SynapseLinkMergeBatch("test.staged_a", batchSchema, "test.table_a", tablePropertiesSettings)
      val expected = Using(Source.fromURL(getClass.getResource(s"/$expectation.sql"))) {
        _.getLines().mkString("\n")
      }.get

      batch.batchQuery.query should equal(expected)
    }
  }
