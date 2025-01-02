package com.sneaksanddata.arcane.framework
package services.streaming.graph_builders

import models.DataRow
import models.settings.VersionedDataGraphBuilderSettings
import services.app.base.StreamLifetimeService
import services.mssql.MsSqlConnection.{DataBatch, VersionedBatch}
import services.mssql.given_HasVersion_VersionedBatch
import services.streaming.base.{BatchProcessor, StreamGraphBuilder, VersionedDataProvider}
import services.streaming.consumers.StreamingConsumer

import org.slf4j.{Logger, LoggerFactory}
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, Schedule, ZIO}

/**
 * The stream graph builder that reads the changes from the database.
 * @param versionedDataGraphBuilderSettings The settings for the stream source.
 * @param versionedDataProvider The versioned data provider.
 * @param streamLifetimeService The stream lifetime service.
 * @param batchProcessor The batch processor.
 */
class VersionedDataGraphBuilder(versionedDataGraphBuilderSettings: VersionedDataGraphBuilderSettings,
                                versionedDataProvider: VersionedDataProvider[Long, VersionedBatch],
                                streamLifetimeService: StreamLifetimeService,
                                batchProcessor: BatchProcessor[DataBatch, Chunk[DataRow]],
                                batchConsumer: StreamingConsumer)
  extends StreamGraphBuilder:

  private val logger: Logger = LoggerFactory.getLogger(classOf[VersionedDataGraphBuilder])
  override type StreamElementType = Chunk[DataRow]

  /**
   * Builds a stream that reads the changes from the database.
   *
   * @return The stream that reads the changes from the database.
   */
  override def create: ZStream[Any, Throwable, StreamElementType] = this.createStream.via(this.batchProcessor.process)

  /**
   * Creates a ZStream for the stream graph.
   *
   * @return ZStream (stream source for the stream graph).
   */
  override def consume: ZSink[Any, Throwable, Chunk[DataRow], Any, Unit] = batchConsumer.consume

  private def createStream = ZStream
    .unfoldZIO(versionedDataProvider.firstVersion) { previousVersion =>
      if streamLifetimeService.cancelled then
        ZIO.succeed(None)
      else
        continueStream(previousVersion)
    }
    .schedule(Schedule.spaced(versionedDataGraphBuilderSettings.changeCaptureInterval))

  private def continueStream(previousVersion: Option[Long]): ZIO[Any, Throwable, Some[(DataBatch, Option[Long])]] =
    versionedDataProvider.requestChanges(previousVersion, versionedDataGraphBuilderSettings.lookBackInterval) map { versionedBatch  =>
      logger.info(s"Received versioned batch: ${versionedBatch.getLatestVersion}")
      val latestVersion = versionedBatch.getLatestVersion
      val (queryResult, _) = versionedBatch
      logger.info(s"Latest version: ${versionedBatch.getLatestVersion}")
      Some(queryResult, latestVersion)
    }

/**
 * The companion object for the VersionedDataGraphBuilder class.
 */
object VersionedDataGraphBuilder:
  type Environment = VersionedDataProvider[Long, VersionedBatch]
    & StreamLifetimeService
    & BatchProcessor[DataBatch, Chunk[DataRow]]
    & StreamingConsumer
    & VersionedDataGraphBuilderSettings

  /**
   * Creates a new instance of the BackfillDataGraphBuilder class.
   *
   * @param versionedDataProvider  The backfill data provider.
   * @param streamLifetimeService The stream lifetime service.
   * @param batchProcessor        The batch processor.
   * @return A new instance of the BackfillDataGraphBuilder class.
   */
  def apply(versionedDataGraphBuilderSettings: VersionedDataGraphBuilderSettings,
             versionedDataProvider: VersionedDataProvider[Long, VersionedBatch],
            streamLifetimeService: StreamLifetimeService,
            batchProcessor: BatchProcessor[DataBatch, Chunk[DataRow]],
            batchConsumer: StreamingConsumer): VersionedDataGraphBuilder =
    new VersionedDataGraphBuilder(versionedDataGraphBuilderSettings,
      versionedDataProvider,
      streamLifetimeService,
      batchProcessor,
      batchConsumer)

  /**
   * Creates a new instance of the BackfillDataGraphBuilder using services provided by ZIO Environment.
   *
   * @return A new instance of the BackfillDataGraphBuilder class.
   */
  def apply(): ZIO[Environment, Nothing, VersionedDataGraphBuilder] =
    for
      _ <- ZIO.log("Running in streaming mode")
      sss <- ZIO.service[VersionedDataGraphBuilderSettings]
      dp <- ZIO.service[VersionedDataProvider[Long, VersionedBatch]]
      ls <- ZIO.service[StreamLifetimeService]
      bp <- ZIO.service[BatchProcessor[DataBatch, Chunk[DataRow]]]
      bc <- ZIO.service[StreamingConsumer]
    yield VersionedDataGraphBuilder(sss, dp, ls, bp, bc)
    

