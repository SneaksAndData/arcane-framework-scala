package com.sneaksanddata.arcane.framework
package utils

import models.DataRow
import zio.Chunk

/**
 * Utility functions for Scala collections
 */
object CollectionUtils:

  /**
   * Merges to maps containing ZIO chunks as values
   * @param mapA First map
   * @param mapB Second map
   * @tparam K Map key type
   * @tparam V Chunk value type
   * @return A map of K, Chunk[V], containing keys from mapA and mapB (merged), and for intersecting keys their respective values are also merged into a new collection.
   */
  def mergeGroupedChunks[K, V](mapA: Map[K, Chunk[V]], mapB: Map[K, Chunk[V]]): Map[K, Chunk[V]] =
    (mapA.toSeq ++ mapB).groupMap(_._1)(_._2).map {
      case (key, chunks) => key -> Chunk.from(chunks.flatten)
    }

  /**
   * Converts a grouped data row to a single-value map, with row itself wrapped in ZIO chunk.
    */
  extension[K] (enrichedRow: (K, DataRow)) def toChunkMap: Map[K, Chunk[DataRow]] =
    Map(enrichedRow._1 -> Chunk(enrichedRow._2))
