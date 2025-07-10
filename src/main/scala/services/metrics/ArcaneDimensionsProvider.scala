package com.sneaksanddata.arcane.framework
package services.metrics

import extensions.StringExtensions.camelCaseToSnakeCase
import models.app.StreamContext
import services.base.DimensionsProvider

import zio.{ZIO, ZLayer}

import scala.collection.immutable.SortedMap

/** A dimensions provider that provides dimensions for DataDog metrics service.
  *
  * @param streamContext
  *   The stream context.
  */
class ArcaneDimensionsProvider(streamContext: StreamContext) extends DimensionsProvider:
  /** Provides the metrics dimensions.
    *
    * @return
    *   The dimensions.
    */
  def getDimensions: SortedMap[String, String] = SortedMap(
    "arcane.sneaksanddata.com/kind"      -> streamContext.streamKind.camelCaseToSnakeCase,
    "arcane.sneaksanddata.com/mode"      -> getStreamMode(streamContext.IsBackfilling),
    "arcane.sneaksanddata.com/stream_id" -> streamContext.streamId
  )

  private def getStreamMode(isBackfilling: Boolean): String = if isBackfilling then "backfill" else "stream"

/** The companion object for the ArcaneDimensionsProvider class.
  */
object ArcaneDimensionsProvider:
  /** The environment type for the ArcaneDimensionsProvider.
    */
  type Environment = StreamContext

  /** Creates a new instance of the ArcaneDimensionsProvider.
    *
    * @param streamContext
    *   The stream context.
    * @return
    *   The ArcaneDimensionsProvider instance.
    */
  def apply(streamContext: StreamContext): ArcaneDimensionsProvider = new ArcaneDimensionsProvider(streamContext)

  /** The ZLayer that creates the ArcaneDimensionsProvider.
    */
  val layer: ZLayer[Environment, Nothing, DimensionsProvider] =
    ZLayer {
      for context <- ZIO.service[StreamContext]
      yield ArcaneDimensionsProvider(context)
    }
