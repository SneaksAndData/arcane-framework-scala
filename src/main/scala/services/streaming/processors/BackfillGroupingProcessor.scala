package com.sneaksanddata.arcane.framework
package services.streaming.processors

import models.DataRow
import models.settings.GroupingSettings
import services.streaming.base.BatchProcessor

import zio.stream.ZPipeline
import zio.{Chunk, ZIO, ZLayer}

import scala.concurrent.duration.Duration

/**
 * The batch processor implementation that converts a lazy DataBatch to a Chunk of DataRow.
 * @param groupingSettings The grouping settings.
 */
class BackfillGroupingProcessor(groupingSettings: GroupingSettings) extends BatchProcessor[DataRow, Chunk[DataRow]]:

  /**
   * Processes the incoming data.
   *
   * @return ZPipeline (stream source for the stream graph).
   */
  def process: ZPipeline[Any, Throwable, DataRow, Chunk[DataRow]] =
    ZPipeline.groupedWithin(groupingSettings.rowsPerGroup, groupingSettings.groupingInterval)


/**
 * The companion object for the LazyOutputDataProcessor class.
 */
object BackfillGroupingProcessor:

  /**
   * The ZLayer that creates the LazyOutputDataProcessor.
   */
  val layer: ZLayer[GroupingSettings, Nothing, BatchProcessor[DataRow, Chunk[DataRow]]] =
    ZLayer {
      for
        settings <- ZIO.service[GroupingSettings]
      yield BackfillGroupingProcessor(settings)
    }

  def apply(groupingSettings: GroupingSettings): BackfillGroupingProcessor =
    require(groupingSettings.rowsPerGroup > 0, "Rows per group must be greater than 0")
    require(!groupingSettings.groupingInterval.equals(Duration.Zero), "groupingInterval must be greater than 0")
    new BackfillGroupingProcessor(groupingSettings)
