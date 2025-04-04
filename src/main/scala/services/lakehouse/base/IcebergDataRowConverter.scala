package com.sneaksanddata.arcane.framework
package services.lakehouse.base

import models.DataRow

/**
 * IcebergConverter trait for converting data rows to the format that can be used by IcebergCatalogWriter.
 */
trait IcebergDataRowConverter:

  /**
   * Converts a DataRow to a Map that can be used by IcebergCatalogWriter to create a data record.
   *
   * @param dataRow The DataRow to convert.
   * @return A Map of Field names to values that can be used by IcebergCatalogWriter.
   */
  def convert(dataRow: DataRow): Map[String, Any]