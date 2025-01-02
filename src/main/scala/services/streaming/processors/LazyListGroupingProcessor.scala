package com.sneaksanddata.arcane.framework
package services.streaming.processors

import models.DataRow
import models.settings.GroupingSettings
import services.mssql.MsSqlConnection.DataBatch
import services.streaming.base.BatchProcessor

import org.slf4j.{Logger, LoggerFactory}
import zio.stream.ZPipeline
import zio.{Chunk, ZIO, ZLayer}

import scala.concurrent.duration.Duration
import scala.util.{Try, Using}

/**
 * The batch processor implementation that converts a lazy DataBatch to a Chunk of DataRow.
 * @param groupingSettings The grouping settings.
 */
class LazyListGroupingProcessor(groupingSettings: GroupingSettings) extends BatchProcessor[DataBatch, Chunk[DataRow]]:

  /**
   * Processes the incoming data.
   *
   * @return ZPipeline (stream source for the stream graph).
   */
  def process: ZPipeline[Any, Throwable, DataBatch, Chunk[DataRow]] = ZPipeline
      .map(this.readBatch)
      // We use here unsafe get because we need to throw an exception if the data is not available.
      // Otherwise, we can get inconsistent data in the stream.
      .map(tryDataRow => tryDataRow.get)
      .map(list => Chunk.fromIterable(list))
      .flattenChunks
      .groupedWithin(groupingSettings.rowsPerGroup, groupingSettings.groupingInterval)

  private def readBatch(dataBatch: DataBatch): Try[List[DataRow]] = Using(dataBatch) { data => data.read.toList }

/**
 * The companion object for the LazyOutputDataProcessor class.
 */
object LazyListGroupingProcessor:

  /**
   * The ZLayer that creates the LazyOutputDataProcessor.
   */
  val layer: ZLayer[GroupingSettings, Nothing, LazyListGroupingProcessor] =
    ZLayer {
      for
        settings <- ZIO.service[GroupingSettings]
      yield LazyListGroupingProcessor(settings)
    }

  def apply(groupingSettings: GroupingSettings): LazyListGroupingProcessor =
    require(groupingSettings.rowsPerGroup > 0, "Rows per group must be greater than 0")
    require(!groupingSettings.groupingInterval.equals(Duration.Zero), "groupingInterval must be greater than 0")
    new LazyListGroupingProcessor(groupingSettings)
