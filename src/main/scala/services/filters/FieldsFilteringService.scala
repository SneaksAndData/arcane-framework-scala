package com.sneaksanddata.arcane.framework
package services.filters

import models.app.PluginStreamContext
import models.schemas.{ArcaneSchema, DataRow}
import models.settings.{ExcludeFieldsImpl, FieldSelectionRuleSettings, IncludeFieldsImpl}
import services.filters.FieldsFilteringService.isValid

import zio.{ZIO, ZLayer}

/** The service that filters the fields of a DataRow or ArcaneSchema.
  */
class FieldsFilteringService(fieldSelectionRule: FieldSelectionRuleSettings):

  require(
    fieldSelectionRule.isValid,
    "The field selection rule must not exclude essential fields: " + fieldSelectionRule.essentialFields.mkString(", ")
  )

  /** Applies field filter to the DataRow instance.
    *
    * @param row
    *   The data to filter.
    * @return
    *   The filtered data/schema.
    */
  def filter(row: DataRow): DataRow = if row.isWatermark then row
  else
    (fieldSelectionRule.isServerSide, fieldSelectionRule.rule) match
      case (false, IncludeFieldsImpl(includeFields)) =>
        row.filter(rowCell => includeFields.fields.exists(f => rowCell.name.toLowerCase().equalsIgnoreCase(f)))
      case (false, ExcludeFieldsImpl(excludeFields)) =>
        row.filter(rowCell => !excludeFields.fields.exists(f => rowCell.name.toLowerCase().equalsIgnoreCase(f)))
      case _ => row

  /** Applies field filter to the ArcaneSchema instance.
    * @param schema
    *   The data to filter.
    * @return
    *   The filtered data/schema.
    */
  def filter(schema: ArcaneSchema): ArcaneSchema = (fieldSelectionRule.isServerSide, fieldSelectionRule.rule) match
    case (false, IncludeFieldsImpl(includeFields)) =>
      schema.filter(field => includeFields.fields.exists(f => field.name.toLowerCase().equalsIgnoreCase(f)))
    case (false, ExcludeFieldsImpl(excludeFields)) =>
      schema.filter(field => !excludeFields.fields.exists(f => field.name.toLowerCase().equalsIgnoreCase(f)))
    case _ => schema

object FieldsFilteringService:

  type Environment = PluginStreamContext

  def apply(fieldSelectionRule: FieldSelectionRuleSettings): FieldsFilteringService = new FieldsFilteringService(
    fieldSelectionRule
  )

  /** The ZLayer that creates the IcebergConsumer.
    */
  val layer: ZLayer[Environment, Nothing, FieldsFilteringService] =
    ZLayer {
      for context <- ZIO.service[PluginStreamContext]
      yield FieldsFilteringService(context.source.fieldSelectionRule)
    }

  /** Validates that the field selection rule does not exclude essential fields
    */
  extension (fieldSelectionRule: FieldSelectionRuleSettings)
    def isValid: Boolean = fieldSelectionRule.rule match
      case IncludeFieldsImpl(includeFields) => fieldSelectionRule.essentialFields.subsetOf(includeFields.fields)
      case ExcludeFieldsImpl(excludeFields) =>
        excludeFields.fields.intersect(fieldSelectionRule.essentialFields).isEmpty
      case _ => true
