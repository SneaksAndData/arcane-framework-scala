package com.sneaksanddata.arcane.framework
package tests.pullstream

import services.pullstream.backfill.NoopBackfillStreamDataProvider

import zio.{LogLevel, Scope}
import zio.test.*

object PullStreamBackfillStubsTests extends ZIOSpecDefault:

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("PullStreamBackfillStubsTests")(
    test("noop backfill provider succeeds with an empty stream") {
      // Regression: this used to fail with an unsupported-operation error, failing every backfill deployment.
      for
        result <- NoopBackfillStreamDataProvider.stream.runCollect.exit
        logs   <- ZTestLogger.logOutput
      yield assertTrue(result.isSuccess)
        && assertTrue(result.exists(_.isEmpty))
        && assertTrue(logs.exists(entry => entry.logLevel == LogLevel.Warning && entry.message().contains("no-op")))
    }
  )
