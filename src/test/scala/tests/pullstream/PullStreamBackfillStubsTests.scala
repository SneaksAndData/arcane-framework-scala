package com.sneaksanddata.arcane.framework
package tests.pullstream

import services.pullstream.backfill.{NoopBackfillStreamDataProvider, NoopShardedBackfillStreamDataProvider}

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
    },
    test("noop sharded backfill provider succeeds with no shards") {
      // Regression: backfill OVERWRITE mode failed with an unsupported-operation error, restarting the stream forever.
      for
        result <- NoopShardedBackfillStreamDataProvider.backfillStream.flatMap(_.stream.runCollect).exit
        logs   <- ZTestLogger.logOutput
      yield assertTrue(result.isSuccess)
        && assertTrue(result.exists(_.isEmpty))
        && assertTrue(logs.exists(entry => entry.logLevel == LogLevel.Warning && entry.message().contains("no-op")))
    }
  )
