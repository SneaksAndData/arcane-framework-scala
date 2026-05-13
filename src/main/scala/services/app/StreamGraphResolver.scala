package com.sneaksanddata.arcane.framework
package services.app

import logging.ZIOLogAnnotations.zlog
import models.app.PluginStreamContext
import models.settings.backfill.BackfillBehavior
import services.backfill.graph.{GenericBackfillMergeGraphBuilder, GenericBackfillOverwriteGraphBuilder}
import services.streaming.base.StreamingGraphBuilder
import services.streaming.graph.DefaultStreamingGraphBuilder

import zio.{ZIO, ZLayer}

/** A factory that creates a graph builder based on the backfill settings and the stream context.
  */
object StreamGraphResolver:

  /** The environment required for the graph builder to be created.
    */
  type Environment = GenericBackfillMergeGraphBuilder.Environment & GenericBackfillOverwriteGraphBuilder.Environment &
    DefaultStreamingGraphBuilder.Environment & PluginStreamContext

  /** The ZLayer for the graph builder injection with runtime dependency resolution.
    */
  val composedLayer: ZLayer[Environment, Nothing, StreamingGraphBuilder] =
    DefaultStreamingGraphBuilder.layer
      >+> GenericBackfillOverwriteGraphBuilder.layer
      >+> GenericBackfillMergeGraphBuilder.layer
      >>> ZLayer.fromZIO(resolveGraphBuilder)

  private type ResolverEnvironment = Environment & GenericBackfillMergeGraphBuilder &
    GenericBackfillOverwriteGraphBuilder & DefaultStreamingGraphBuilder

  private def resolveGraphBuilder: ZIO[ResolverEnvironment, Nothing, StreamingGraphBuilder] =
    for
      context <- ZIO.service[PluginStreamContext]

      _             <- zlog("resoling graph builder using stream context and backfill settings")
      isBackfilling <- context.isBackfilling.orElseSucceed(false)
      builder <- (isBackfilling, context.streamMode.backfill.backfillBehavior) match
        case (false, _)                         => ZIO.service[DefaultStreamingGraphBuilder]
        case (true, BackfillBehavior.Merge)     => ZIO.service[GenericBackfillMergeGraphBuilder]
        case (true, BackfillBehavior.Overwrite) => ZIO.service[GenericBackfillOverwriteGraphBuilder]
      _ <- zlog("Using the stream graph builder: %s", builder.getClass.getName)
    yield builder
