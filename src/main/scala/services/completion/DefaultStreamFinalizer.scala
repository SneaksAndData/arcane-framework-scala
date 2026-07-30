package com.sneaksanddata.arcane.framework
package services.completion

import logging.ZIOLogAnnotations.zlog
import models.app.PluginStreamContext
import services.base.StreamingSource
import services.completion.base.StreamFinalizer
import services.iceberg.base.StagingEntityManager
import services.naming.NameGenerator

import zio.{Task, ZIO, ZLayer}

class DefaultStreamFinalizer(
    stagingEntityManager: StagingEntityManager,
    streamingSource: StreamingSource,
    nameGenerator: NameGenerator,
    isBackfilling: Boolean
) extends StreamFinalizer:
  /** Backfill finalizer
    */
  override def finalizeBackfill: Task[Unit] = for _ <- ZIO.when(isBackfilling) {
      for
        _      <- zlog("Looking for backfill tables created by the current run")
        prefix <- nameGenerator.getBackfillTablesPrefix.map(v => s"${v}__")
        _      <- stagingEntityManager.deleteTables(prefix)
        _      <- streamingSource.deleteShards(prefix)
      yield ()
    }
  yield ()

  /** Change capture stream finalizer
    *
    * @return
    */
  override def finalizeChangeCapture: Task[Unit] = ZIO.unit

object DefaultStreamFinalizer:
  val layer = ZLayer {
    for
      context              <- ZIO.service[PluginStreamContext]
      stagingEntityManager <- ZIO.service[StagingEntityManager]
      streamingSource      <- ZIO.service[StreamingSource]
      isBackfilling        <- context.isBackfilling.orElseSucceed(false)
      nameGenerator        <- ZIO.service[NameGenerator]
    yield DefaultStreamFinalizer(
      stagingEntityManager = stagingEntityManager,
      streamingSource = streamingSource,
      nameGenerator = nameGenerator,
      isBackfilling = isBackfilling
    )
  }
