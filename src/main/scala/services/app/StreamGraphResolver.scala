package com.sneaksanddata.arcane.framework
package services.app

import logging.ZIOLogAnnotations.zlog
import models.app.PluginStreamContext
import models.settings.backfill.BackfillBehavior
import services.backfill.graph.{DefaultBackfillMergeGraphBuilder, DefaultBackfillOverwriteGraphBuilder}
import services.streaming.base.StreamingGraphBuilder
import services.streaming.graph.DefaultStreamingGraphBuilder

import com.sneaksanddata.arcane.framework.services.migration.graph.DefaultMigrationGraphBuilder
import zio.{ZIO, ZLayer}

/** A factory that creates a graph builder based on the backfill settings and the stream context.
  */
object StreamGraphResolver:

  /** The environment required for the graph builder to be created.
    */
  type Environment = DefaultBackfillOverwriteGraphBuilder.Environment & DefaultStreamingGraphBuilder.Environment &
    DefaultBackfillMergeGraphBuilder.Environment & DefaultMigrationGraphBuilder.Environment & PluginStreamContext

  /** The ZLayer for the graph builder injection with runtime dependency resolution.
    */
  val composedLayer: ZLayer[Environment, Throwable, StreamingGraphBuilder] =
    DefaultStreamingGraphBuilder.layer
      >+> DefaultBackfillOverwriteGraphBuilder.layer
      >+> DefaultBackfillMergeGraphBuilder.layer
      >+> DefaultMigrationGraphBuilder.layer
      >>> ZLayer.fromZIO(resolveGraphBuilder)

  private type ResolverEnvironment = Environment & DefaultBackfillOverwriteGraphBuilder & DefaultStreamingGraphBuilder &
    DefaultBackfillMergeGraphBuilder & DefaultMigrationGraphBuilder

  private def resolveGraphBuilder: ZIO[ResolverEnvironment, Throwable, StreamingGraphBuilder] =
    for
      context <- ZIO.service[PluginStreamContext]

      _             <- zlog("resoling graph builder using stream context and backfill settings")
      isBackfilling <- context.isBackfilling.orElseSucceed(false)
      isMigrating   <- context.isMigrating
      builder <- (isMigrating, isBackfilling, context.streamMode.backfill.backfillBehavior) match
        case (true, _, _)                              => ZIO.service[DefaultMigrationGraphBuilder]
        case (false, false, _)                         => ZIO.service[DefaultStreamingGraphBuilder]
        case (false, true, BackfillBehavior.Merge)     => ZIO.service[DefaultBackfillMergeGraphBuilder]
        case (false, true, BackfillBehavior.Overwrite) => ZIO.service[DefaultBackfillOverwriteGraphBuilder]
      _ <- zlog("Using the stream graph builder: %s", builder.getClass.getName)
    yield builder
