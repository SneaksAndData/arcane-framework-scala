package com.sneaksanddata.arcane.framework
package services.streaming.base

import models.DataRow

/**
 * A trait that represents a stream item that can contain stream of data rows interleaved with metadata of
 * any other kind.
 */
trait MetadataEnrichedRowStreamElement[-A]:
  /**
   * Checks if the element is a data row.
   *
   * @return True if the element is a data row, false otherwise.
   */
  extension (a: A) def isDataRow: Boolean

  /**
   * Converts the element to a data row.
   *
   * @return The data row.
   */
  extension (a: A) def toDataRow: DataRow
