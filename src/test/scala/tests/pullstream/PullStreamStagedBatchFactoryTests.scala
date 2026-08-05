package com.sneaksanddata.arcane.framework
package tests.pullstream

import models.schemas.{ArcaneSchema, ArcaneType, Field, MergeKeyField}
import services.pullstream.{MissingVersionFieldException, PullStreamStagedBatchFactory}

import zio.Scope
import zio.test.*

object PullStreamStagedBatchFactoryTests extends ZIOSpecDefault:

  private val watermarkField = "timestampUTC"

  private def schemaWith(fields: Seq[String]): ArcaneSchema =
    ArcaneSchema(fields.map(name => Field(name, ArcaneType.StringType)) :+ MergeKeyField)

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("PullStreamStagedBatchFactoryTests")(
    test("orders versions by the watermark column when the sink declares it") {
      val factory = PullStreamStagedBatchFactory(watermarkField)
      for batch <- factory.createDataBatch("staged", "target", schemaWith(Seq("id", "payload", watermarkField)))
      yield assertTrue(batch.reduceExpr.contains(s"ORDER BY $watermarkField DESC"))
    },
    test("uses the sink spelling when it differs from the configured watermark field") {
      // Engines that fold unquoted identifiers cannot tell the two spellings apart, so neither should we.
      val factory = PullStreamStagedBatchFactory(watermarkField)
      for batch <- factory.createDataBatch("staged", "target", schemaWith(Seq("id", "TimestampUTC")))
      yield assertTrue(batch.reduceExpr.contains("ORDER BY TimestampUTC DESC"))
    },
    test("fails with an actionable error when the sink has no watermark column") {
      // Regression: this used to emit a MERGE referencing an unknown column, surfacing as an opaque
      // "Column 'timestamputc' cannot be resolved" failure from the query engine.
      val factory = PullStreamStagedBatchFactory(watermarkField)
      for error <- factory.createDataBatch("staged", "target", schemaWith(Seq("id", "payload"))).flip
      yield assertTrue(error.isInstanceOf[MissingVersionFieldException])
        && assertTrue(error.getMessage.contains(watermarkField))
        && assertTrue(error.getMessage.contains("id, payload"))
    }
  )
