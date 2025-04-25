package com.sneaksanddata.arcane.framework
package services.mssql.base

import services.mssql.ColumnSummary
import scala.util.Try

/**
 * Trait for server-side filtering of fields in a Microsoft SQL Server database.
 */
trait MsSqlServerFieldsFilteringService:
  
  /**
   * Filters the given fields based on the implemented filtering logic.
   *
   * @param fields The list of fields to filter.
   * @return A Try containing the filtered list of fields or an error if filtering fails.
   */
  def filter(fields: List[ColumnSummary]): Try[List[ColumnSummary]]
