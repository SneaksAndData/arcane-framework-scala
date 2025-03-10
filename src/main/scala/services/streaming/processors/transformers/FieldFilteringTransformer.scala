package com.sneaksanddata.arcane.framework
package services.streaming.processors.transformers

import models.settings.{FieldSelectionRule, FieldSelectionRuleSettings}
import models.{ArcaneSchema, DataCell, DataRow, given_NamedCell_DataCell}
import services.base.SchemaProvider
import services.filters.FieldsFilteringService
import services.streaming.base.{MetadataEnrichedRowStreamElement, RowProcessor}

import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

/**
 * The transformer implementation that filters the fields of a DataRow.
 */
class FieldFilteringTransformer(fieldsFilteringService: FieldsFilteringService) extends RowProcessor:

  /**
   * @inheritdoc
   */
  override def process[Element: MetadataEnrichedRowStreamElement]: ZPipeline[Any, Throwable, Element, Element] = ZPipeline.map {
    case row if row.isDataRow => fieldsFilteringService.filter[DataCell](row.toDataRow).asInstanceOf[DataRow].fromDataRow
    case other if !other.isDataRow => other
  }

/**
 * The companion object.
 */
object FieldFilteringTransformer:
  
  /**
   * The environment type.
   */
  type Environment = FieldsFilteringService

  /**
   * Creates a new FieldFilteringTransformer.
   *
   * @param fieldSelectionService The field selection service.
   * @return The FieldFilteringTransformer.
   */
  def apply(fieldSelectionService: FieldsFilteringService): FieldFilteringTransformer =
    new FieldFilteringTransformer(fieldSelectionService)

  /**
   * The ZLayer that creates the IcebergConsumer.
   */
  val layer: ZLayer[Environment, Nothing, FieldFilteringTransformer] =
    ZLayer {
      for fieldSelectionRule <- ZIO.service[FieldsFilteringService]
      yield FieldFilteringTransformer(fieldSelectionRule)
    }
