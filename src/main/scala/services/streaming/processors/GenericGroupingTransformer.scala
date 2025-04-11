package com.sneaksanddata.arcane.framework
package services.streaming.processors

import logging.ZIOLogAnnotations.*
import models.settings.GroupingSettings
import services.base.DeclaredMetrics
import services.streaming.base.GroupingTransformer

import zio.*
import zio.stream.ZPipeline

import scala.concurrent.duration.Duration

/**
 * @inheritdoc
 */
class GenericGroupingTransformer(groupingSettings: GroupingSettings) extends GroupingTransformer:

  /**
   * @inheritdoc
   */
  def process: ZPipeline[Any, Throwable, Element, Chunk[Element]] = ZPipeline
    .groupedWithin(groupingSettings.rowsPerGroup, groupingSettings.groupingInterval)
    .mapZIO(logBatchSize)

  private def logBatchSize(batch: Chunk[Element]) =
    for 
      size <- ZIO.succeed(batch.size.toLong) @@ DeclaredMetrics.rowsIncoming
      _ <- zlog(s"Received batch with %s rows from streaming source", size.toString)
    yield batch
    
/**
 * The companion object for the LazyOutputDataProcessor class.
 */
object GenericGroupingTransformer:
  
  type Environment = GroupingSettings 

  /**
   * The ZLayer that creates the LazyOutputDataProcessor.
   */
  val layer: ZLayer[Environment, Nothing, GenericGroupingTransformer] =
    ZLayer {
      for
        settings <- ZIO.service[GroupingSettings]
      yield GenericGroupingTransformer(settings)
    }

  def apply(groupingSettings: GroupingSettings): GenericGroupingTransformer =
    require(groupingSettings.rowsPerGroup > 0, "Rows per group must be greater than 0")
    require(!groupingSettings.groupingInterval.equals(Duration.Zero), "groupingInterval must be greater than 0")
    new GenericGroupingTransformer(groupingSettings)
