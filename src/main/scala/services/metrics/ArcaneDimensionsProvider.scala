package com.sneaksanddata.arcane.framework
package services.metrics

import extensions.StringExtensions.camelCaseToSnakeCase
import models.app.BaseStreamContext
import services.base.DimensionsProvider

import zio.{ZIO, ZLayer}

import scala.collection.immutable.SortedMap

/** A dimensions provider that provides dimensions for DataDog metrics service.
  *
  * @param streamContext
  *   The stream context.
  */
class ArcaneDimensionsProvider(streamContext: BaseStreamContext) extends DimensionsProvider:
  private val dimensionPrefix = "arcane.sneaksanddata.com"

  /** Provides the metrics dimensions.
    *
    * @return
    *   The dimensions.
    */
  def getDimensions: SortedMap[String, String] = SortedMap(
    s"$dimensionPrefix/kind"      -> streamContext.streamKind.camelCaseToSnakeCase,
    s"$dimensionPrefix/mode"      -> getStreamMode(streamContext.IsBackfilling),
    s"$dimensionPrefix/stream_id" -> streamContext.streamId
  ) ++ streamContext.customTags.map { case (tagKey, tagValue) =>
    s"$dimensionPrefix/$tagKey" -> tagValue
  }

  private def getStreamMode(isBackfilling: Boolean): String = if isBackfilling then "backfill" else "stream"

/** The companion object for the ArcaneDimensionsProvider class.
  */
object ArcaneDimensionsProvider:
  /** The environment type for the ArcaneDimensionsProvider.
    */
  type Environment = BaseStreamContext

  /** Creates a new instance of the ArcaneDimensionsProvider.
    *
    * @param streamContext
    *   The stream context.
    * @return
    *   The ArcaneDimensionsProvider instance.
    */
  def apply(streamContext: BaseStreamContext): ArcaneDimensionsProvider = new ArcaneDimensionsProvider(streamContext)

  /** The ZLayer that creates the ArcaneDimensionsProvider.
    */
  val layer: ZLayer[Environment, Nothing, DimensionsProvider] =
    ZLayer {
      for context <- ZIO.service[BaseStreamContext]
      yield ArcaneDimensionsProvider(context)
    }
