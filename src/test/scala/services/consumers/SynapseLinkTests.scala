package com.sneaksanddata.arcane.framework
package services.consumers

import models.ArcaneType.{LongType, StringType}
import models.{Field, MergeKeyField}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.io.Source
import scala.util.Using

class SynapseLinkTests extends AnyFlatSpec with Matchers:

  it should "generate a valid overwrite query" in {
    val query = SynapseLinkBackfillQuery("test.table_a",
      """SELECT * FROM (
        | SELECT * FROM test.staged_a ORDER BY ROW_NUMBER() OVER (PARTITION BY Id ORDER BY versionnumber DESC) FETCH FIRST 1 ROWS WITH TIES
        |) WHERE IsDelete = false""".stripMargin)
    val expected = Using(Source.fromURL(getClass.getResource("/generate_an_overwrite_query_synapse_link.sql"))) {
      _.getLines().mkString("\n")
    }.get
    query.query should equal(expected)
  }

  it should "generate a valid merge query" in {
    val query = SynapseLinkMergeQuery(
      "test.table_a",
      """SELECT * FROM (
        | SELECT * FROM test.staged_a ORDER BY ROW_NUMBER() OVER (PARTITION BY Id ORDER BY versionnumber DESC) FETCH FIRST 1 ROWS WITH TIES
        |)""".stripMargin,
      Map(),
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
        | SELECT * FROM test.staged_a ORDER BY ROW_NUMBER() OVER (PARTITION BY Id ORDER BY versionnumber DESC) FETCH FIRST 1 ROWS WITH TIES
        |)""".stripMargin,
      Map(
        "colA" -> List("a", "b", "c")
      ),
      "ARCANE_MERGE_KEY",
      Seq("ARCANE_MERGE_KEY", "colA", "colB", "Id", "versionnumber")
    )

    val expected = Using(Source.fromURL(getClass.getResource("/generate_a_valid_merge_query_with_partitions_synapse_link.sql"))) {
      _.getLines().mkString("\n")
    }.get

    query.query should equal(expected)
  }

  "SynapseLinkBackfillBatch" should "generate a valid backfill batch" in {
    val batch = SynapseLinkBackfillBatch("test.staged_a", Seq(
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
    ), "test.table_a")

    val expected = Using(Source.fromURL(getClass.getResource("/generate_a_valid_synapse_link_backfill_batch_query.sql"))) {
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
      Map("colA" -> List("a", "b", "c")
      ))

    val expected = Using(Source.fromURL(getClass.getResource("/generate_a_valid_synapse_link_merge_query_with_partitions.sql"))) {
      _.getLines().mkString("\n")
    }.get

    batch.batchQuery.query should equal(expected)
  }