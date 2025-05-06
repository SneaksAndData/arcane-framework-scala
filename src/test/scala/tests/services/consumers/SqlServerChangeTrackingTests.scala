package com.sneaksanddata.arcane.framework
package tests.services.consumers

import models.ArcaneType.StringType
import models.batches.{SqlServerChangeTrackingBackfillBatch, SqlServerChangeTrackingBackfillQuery, SqlServerChangeTrackingMergeBatch, SqlServerChangeTrackingMergeQuery}
import models.{Field, MergeKeyField}
import tests.shared.{CustomTablePropertiesSettings, TestTablePropertiesSettings}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table

import scala.io.Source
import scala.util.Using

class SqlServerChangeTrackingTests extends AnyFlatSpec with Matchers:
  
  it should "generate a valid overwrite query" in {
    val query = SqlServerChangeTrackingBackfillQuery("test.table_a", "SELECT * FROM test.staged_a", TestTablePropertiesSettings)
    val expected = Using(Source.fromURL(getClass.getResource("/generate_an_overwrite_query.sql"))) { _.getLines().mkString("\n") }.get
    query.query should equal(expected)
  }

  it should "generate a valid merge query" in {
    val query = SqlServerChangeTrackingMergeQuery(
      "test.table_a",
      "SELECT * FROM test.staged_a",
      Seq(),
      "ARCANE_MERGE_KEY",
      Seq("ARCANE_MERGE_KEY", "colA", "colB")
    )

    val expected = Using(Source.fromURL(getClass.getResource("/generate_a_valid_merge_query.sql"))) { _.getLines().mkString("\n") }.get
    query.query should equal(expected)
  }

  it should "generate a valid merge with partitions" in {
    val query = SqlServerChangeTrackingMergeQuery(
      "test.table_a",
      "SELECT * FROM test.staged_a",
      Seq("colA"),
      "ARCANE_MERGE_KEY",
      Seq("ARCANE_MERGE_KEY", "colA", "colB")
    )

    val expected = Using(Source.fromURL(getClass.getResource("/generate_a_valid_merge_query_with_partitions.sql"))) {
      _.getLines().mkString("\n")
    }.get

    query.query should equal(expected)
  }

  "SqlServerChangeTrackingBackfillBatch" should "generate a valid backfill batch" in {
    val batch = SqlServerChangeTrackingBackfillBatch("test.staged_a", Seq(
      MergeKeyField,
      Field(
        name = "colA",
        fieldType = StringType
      ),
      Field(
        name = "colB",
        fieldType = StringType
      )
    ), "test.table_a", "test.archive_table_a", TestTablePropertiesSettings)

    val expected = Using(Source.fromURL(getClass.getResource("/generate_a_valid_sql_ct_backfill_batch_query.sql"))) {
      _.getLines().mkString("\n")
    }.get

    batch.batchQuery.query should equal(expected)
  }

  "SqlServerChangeTrackingMergeBatch" should "generate a valid versioned batch" in {
    val batch = SqlServerChangeTrackingMergeBatch("test.staged_a", Seq(
      MergeKeyField,
        Field(
          name = "colA",
          fieldType = StringType
        ),
        Field(
          name = "colB",
          fieldType = StringType
        )
      ),
      "test.table_a",
      CustomTablePropertiesSettings(Seq("bucket(colA, 32)"))
    )

    val expected = Using(Source.fromURL(getClass.getResource("/generate_a_valid_sql_ct_merge_query_with_partitions.sql"))) {
      _.getLines().mkString("\n")
    }.get

    batch.batchQuery.query should equal(expected)
  }

  private val mergeKeyStatements = Table(
    ("tablePropertiesSettings", "expectedResult"),
    (Seq("bucket(ARCANE_MERGE_KEY, 32)"), "filter_out_single_arcane_merge_key_from_merge_match_sql_server"),
    (Seq("bucket(ARCANE_MERGE_KEY, 32)", "bucket(colA, 32)"), "filter_out_arcane_merge_key_from_merge_match_sql_server")
  )

  "SqlServerChangeTrackingMergeBatch" should "filter out arcane merge key from merge match" in {
    val batchSchema = Seq(
      MergeKeyField,
      Field(
        name = "colA",
        fieldType = StringType
      ),
      Field(
        name = "colB",
        fieldType = StringType
      )
    )
    forAll(mergeKeyStatements) { (partitionSpec, expectation) =>
      val tablePropertiesSettings = CustomTablePropertiesSettings(partitionSpec)
      val batch = SqlServerChangeTrackingMergeBatch("test.staged_a", batchSchema, "test.table_a", tablePropertiesSettings)
      val expected = Using(Source.fromURL(getClass.getResource(s"/$expectation.sql"))) {
        _.getLines().mkString("\n")
      }.get

      batch.batchQuery.query should equal(expected)
    }
  }
