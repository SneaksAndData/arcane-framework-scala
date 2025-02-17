package com.sneaksanddata.arcane.framework
package services.streaming.processors

import models.DataRow
import models.settings.{AllFields, ExcludeFields, FieldSelectionRule, IncludeFields}
import services.streaming.base.BatchProcessor

import zio.Chunk
import zio.stream.ZPipeline

class FieldFilteringProcessor(fieldSelectionRule: FieldSelectionRule) extends BatchProcessor[DataRow, DataRow]:

  def process: ZPipeline[Any, Throwable, DataRow, DataRow] = ZPipeline.map { row =>
    fieldSelectionRule match
      case _: AllFields => row
      case includeFields: IncludeFields => row.filter(entry => includeFields.fields.contains(entry.name))
      case excludeFields: ExcludeFields => row.filter(entry => !excludeFields.fields.contains(entry.name))
  }
