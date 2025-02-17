package com.sneaksanddata.arcane.framework
package services.streaming.processors

import models.DataRow
import models.settings.FieldSelectionRule.IncludeFields
import models.settings.{FieldSelectionRuleSettings, FieldSelectionRule}
import services.streaming.base.BatchProcessor

import zio.Chunk
import zio.stream.ZPipeline

/**
 * The batch processor implementation that filters the fields of a DataRow.
 * @param fieldSelectionRule The field selection rule.
 */
class FieldFilteringProcessor(fieldSelectionRule: FieldSelectionRuleSettings) extends BatchProcessor[DataRow | Any, DataRow | Any]:

  def process: ZPipeline[Any, Throwable, DataRow|Any, DataRow|Any] = ZPipeline.map {
    case row: DataRow => filterFields(row)
    case other => other
  }

  private def filterFields(row: DataRow) = fieldSelectionRule.rule match
    case includeFields: FieldSelectionRule.IncludeFields => row.filter(entry => includeFields.fields.contains(entry.name))
    case excludeFields: FieldSelectionRule.ExcludeFields => row.filter(entry => !excludeFields.fields.contains(entry.name))
    case _ => row

