package com.sneaksanddata.arcane.framework
package services.streaming.processors.transformers

import models.schemas.DataRow
import services.filters.FieldsFilteringService
import services.streaming.base.RowProcessor

import zio.stream.ZPipeline
import zio.{ZIO, ZLayer}

/** The transformer implementation that filters the fields of a DataRow.
  */
class FieldFilteringTransformer(fieldsFilteringService: FieldsFilteringService) extends RowProcessor:

  /** @inheritdoc
    */
  override def process: ZPipeline[Any, Throwable, Element, Element] = ZPipeline.map {
    fieldsFilteringService.filter(_: DataRow)
  }

/** The companion object.
  */
object FieldFilteringTransformer:

  /** The environment type.
    */
  type Environment = FieldsFilteringService

  /** Creates a new FieldFilteringTransformer.
    *
    * @param fieldSelectionService
    *   The field selection service.
    * @return
    *   The FieldFilteringTransformer.
    */
  def apply(fieldSelectionService: FieldsFilteringService): FieldFilteringTransformer =
    new FieldFilteringTransformer(fieldSelectionService)

  /** The ZLayer that creates the IcebergConsumer.
    */
  val layer: ZLayer[Environment, Nothing, FieldFilteringTransformer] =
    ZLayer {
      for fieldSelectionRule <- ZIO.service[FieldsFilteringService]
      yield FieldFilteringTransformer(fieldSelectionRule)
    }
