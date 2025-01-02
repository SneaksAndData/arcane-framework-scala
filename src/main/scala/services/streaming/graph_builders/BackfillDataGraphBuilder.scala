package com.sneaksanddata.arcane.framework
package services.streaming.graph_builders

import models.DataRow
import services.app.base.StreamLifetimeService
import services.streaming.base.{BackfillDataProvider, BatchConsumer, BatchProcessor, StreamGraphBuilder}
import services.streaming.consumers.BackfillConsumer

import org.slf4j.{Logger, LoggerFactory}
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, ZIO}

class BackfillDataGraphBuilder(backfillDataProvider: BackfillDataProvider,
                               streamLifetimeService: StreamLifetimeService,
                               batchProcessor: BatchProcessor[DataRow, Chunk[DataRow]],
                               batchConsumer: BackfillConsumer)
  extends StreamGraphBuilder:


  private val logger: Logger = LoggerFactory.getLogger(classOf[BackfillDataGraphBuilder])

  override type StreamElementType = Chunk[DataRow]

  override def create: ZStream[Any, Throwable, StreamElementType] =
    ZStream.fromZIO(backfillDataProvider.requestBackfill)
      .takeUntil(_ => streamLifetimeService.cancelled)
      .flatMap(batch => ZStream.fromIterable(batch.read))
      .via(batchProcessor.process)

  override def consume: ZSink[Any, Throwable, StreamElementType, Any, Unit] = batchConsumer.consume

/**
 * The companion object for the VersionedDataGraphBuilder class.
 */
object BackfillDataGraphBuilder:
  type Environment = BackfillDataProvider
    & StreamLifetimeService
    & BatchProcessor[DataRow, Chunk[DataRow]]
    & BackfillConsumer

  /**
   * Creates a new instance of the BackfillDataGraphBuilder class.
   *
   * @param backfillDataProvider  The backfill data provider.
   * @param streamLifetimeService The stream lifetime service.
   * @param batchProcessor        The batch processor.
   * @return A new instance of the BackfillDataGraphBuilder class.
   */
  def apply(backfillDataProvider: BackfillDataProvider,
            streamLifetimeService: StreamLifetimeService,
            batchProcessor: BatchProcessor[DataRow, Chunk[DataRow]],
            batchConsumer: BackfillConsumer): BackfillDataGraphBuilder =
    new BackfillDataGraphBuilder(backfillDataProvider, streamLifetimeService, batchProcessor, batchConsumer)

  /**
   * Creates a new instance of the BackfillDataGraphBuilder using services provided by ZIO Environment.
   *
   * @return A new instance of the BackfillDataGraphBuilder class.
   */
  def apply(): ZIO[Environment, Nothing, BackfillDataGraphBuilder] =
    for
      _ <- ZIO.log("Running in backfill mode")
      dp <- ZIO.service[BackfillDataProvider]
      ls <- ZIO.service[StreamLifetimeService]
      bp <- ZIO.service[BatchProcessor[DataRow, Chunk[DataRow]]]
      bc <- ZIO.service[BackfillConsumer]
    yield BackfillDataGraphBuilder(dp, ls, bp, bc)

