package com.sneaksanddata.arcane.framework
package services.app

import logging.ZIOLogAnnotations.zlog
import services.app.base.StreamRunnerService
import services.bootstrap.base.StreamBootstrapper
import services.completion.base.StreamFinalizer
import services.metrics.base.MetricTagProvider
import services.streaming.base.StreamingGraphBuilder
import utils.MetadataUtils

import zio.stream.{ZPipeline, ZSink}
import zio.{Tag, ZIO, ZIOAspect, ZLayer}

import scala.collection.SortedMap

/** A service that can be used to run a stream.
  *
  * @param builder
  *   The stream graph builder.
  */
class GenericStreamRunnerService(
    builder: StreamingGraphBuilder,
    bootstrapper: StreamBootstrapper,
    finalizer: StreamFinalizer,
    tagProvider: MetricTagProvider
) extends StreamRunnerService:

  /** Runs the stream.
    *
    * @return
    *   A ZIO effect that represents the stream.
    */
  def run: ZIO[Any, Throwable, Unit] =
    ZIO
      .attempt(tagProvider.getTags)
      .flatMap(tags =>
        (for
          version <- MetadataUtils.getFrameworkVersion
          _       <- zlog("Starting the stream runner using framework version %s", version)
          _       <- bootstrapper.cleanupStagingTables
          _       <- bootstrapper.cleanupOutdatedBackfill

          _ <- bootstrapper.createTargetTable
          _ <- bootstrapper.createBackFillTable
          _ <- builder.produce().run(logResults)
          _ <- zlog("Stream completed, finalizing")

          _ <- finalizer.finalizeBackfill
          _ <- finalizer.finalizeChangeCapture
        yield ()) @@ ZIOAspect.tagged(Option(tags).getOrElse(SortedMap.empty[String, String]).toList*)
      )

  /** Logs the results of the stream.
    */
  private def logResults = ZSink.foreach(result => zlog("Processing completed: %s", result.toString))

/** The companion object for the StreamRunnerServiceImpl class.
  */
object GenericStreamRunnerService:

  /** The required environment for the GenericStreamRunnerService.
    */
  type Environment = StreamingGraphBuilder & StreamBootstrapper & StreamFinalizer & MetricTagProvider

  /** Creates a new instance of the GenericStreamRunnerService class.
    *
    * @param builder
    *   The stream graph builder.
    * @return
    *   A new instance of the GenericStreamRunnerService class.
    */
  def apply(
      builder: StreamingGraphBuilder,
      bootstrapper: StreamBootstrapper,
      finalizer: StreamFinalizer,
      tagProvider: MetricTagProvider
  ): GenericStreamRunnerService =
    new GenericStreamRunnerService(
      builder,
      bootstrapper,
      finalizer,
      tagProvider
    )

  /** The ZLayer for the GenericStreamRunnerService.
    */
  val layer: ZLayer[Environment, Nothing, StreamRunnerService] =
    ZLayer {
      for
        builder      <- ZIO.service[StreamingGraphBuilder]
        bootstrapper <- ZIO.service[StreamBootstrapper]
        finalizer    <- ZIO.service[StreamFinalizer]
        tagProvider  <- ZIO.service[MetricTagProvider]
      yield GenericStreamRunnerService(
        builder,
        bootstrapper,
        finalizer,
        tagProvider
      )
    }
