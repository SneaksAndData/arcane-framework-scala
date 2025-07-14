package com.sneaksanddata.arcane.framework
package tests.services.consumers

import com.sneaksanddata.arcane.framework.models.batches.{BlobBatchCommons, UpsertBlobBackfillQuery, UpsertBlobMergeQuery}
import com.sneaksanddata.arcane.framework.tests.shared.TestTablePropertiesSettings
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.io.Source
import scala.util.Using

class UpsertBlobTests extends AnyFlatSpec with Matchers:
  it should "generate a valid overwrite query" in {
    val query = UpsertBlobBackfillQuery(
      "test.table_a",
      """SELECT * FROM test.staged_a""".stripMargin,
      TestTablePropertiesSettings
    )
    val expected = Using(Source.fromURL(getClass.getResource("/generated_an_overwrite_query_upsertblob.sql"))) {
      _.getLines().mkString("\n")
    }.get
    query.query should equal(expected)
  }

  it should "generate a valid merge query" in {
    val query = UpsertBlobMergeQuery(
      "test.table_a",
      s"""SELECT * FROM (
        | SELECT * FROM test.staged_a ORDER BY ROW_NUMBER() OVER (PARTITION BY ARCANE_MERGE_KEY ORDER BY ${BlobBatchCommons.versionField.name} DESC) FETCH FIRST 1 ROWS WITH TIES
        |)""".stripMargin,
      Seq(),
      "ARCANE_MERGE_KEY",
      Seq("ARCANE_MERGE_KEY", "colA", "colB", "Id", BlobBatchCommons.versionField.name)
    )

    val expected = Using(Source.fromURL(getClass.getResource("/generate_a_valid_merge_query_upsertblob.sql"))) {
      _.getLines().mkString("\n")
    }.get
    query.query should equal(expected)
  }
