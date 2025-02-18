package com.sneaksanddata.arcane.framework
package services.metrics

import models.app.StreamContext
import services.metrics.ArcaneDimensionsProvider.camelCaseToSnakeCase
import services.metrics.base.DimensionsProvider

import zio.{ZIO, ZLayer}

import scala.annotation.tailrec
import scala.collection.immutable.SortedMap

/**
 * A dimensions provider that provides dimensions for DataDog metrics service.
 *
 * @param streamContext The stream context.
 */
class ArcaneDimensionsProvider(streamContext: StreamContext) extends DimensionsProvider:
  /**
   * Provides the metrics dimensions.
   *
   * @return The dimensions.
   */
  def getDimensions: SortedMap[String, String] = SortedMap(
    "arcane.sneaksanddata.com/kind" -> streamContext.streamKind.camelCaseToSnakeCase,
    "arcane.sneaksanddata.com/mode" -> getStreamMode(streamContext.IsBackfilling),
    "arcane.sneaksanddata.com/stream_id" -> streamContext.streamId,
  )

  private def getStreamMode(isBackfilling: Boolean): String = if isBackfilling then "backfill" else "stream"

/**
 * The companion object for the ArcaneDimensionsProvider class.
 */
object ArcaneDimensionsProvider:
  /**
   * Creates a new instance of the ArcaneDimensionsProvider.
   *
   * @param streamContext The stream context.
   * @return The ArcaneDimensionsProvider instance.
   */
  def apply(streamContext: StreamContext): ArcaneDimensionsProvider = new ArcaneDimensionsProvider(streamContext)

  /**
   * The ZLayer that creates the ArcaneDimensionsProvider.
   */
  val layer: ZLayer[StreamContext, Nothing, DimensionsProvider] =
    ZLayer {
      for
        context <- ZIO.service[StreamContext]
      yield ArcaneDimensionsProvider(context)
    }

  /**
   * Converts a camel case string to a snake case string.
   *
   * @return The converted string.
   */
  extension (s: String) def camelCaseToSnakeCase: String = toSnakeCase(s, 0, new StringBuilder())

  @tailrec
  private def toSnakeCase(s: String, from: Int, acc: StringBuilder): String =
    if from == s.length then
      acc.toString()
    else
      if Character.isUpperCase(s.charAt(from)) && acc.nonEmpty && !Character.isUpperCase(acc.charAt(acc.length-1)) then
        acc.append("_")
      acc.append(java.lang.Character.toLowerCase(s.charAt(from)))
      toSnakeCase(s, from + 1, acc)