package com.sneaksanddata.arcane.framework
package services.consumers

import models.ArcaneType.StringType
import models.settings.TableFormat.PARQUET
import models.settings.{TableFormat, TablePropertiesSettings}
import models.{Field, MergeKeyField}
import utils.{CustomTablePropertiesSettings, TestTablePropertiesSettings}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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
    val batch = SqlServerChangeTrackingMergeBatch("test.staged_a", "batchId", Seq(
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