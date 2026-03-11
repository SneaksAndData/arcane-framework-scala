package com.sneaksanddata.arcane.framework
package services.filters

import models.app.PluginStreamContext
import models.schemas.{ArcaneSchema, DataRow}
import models.settings.{FieldSelectionRule, FieldSelectionRuleSettings, IncludeFields, ExcludeFields}
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
      case (false, IncludeFields(includeFields)) =>
        row.filter(rowCell => includeFields.exists(f => rowCell.name.toLowerCase().equalsIgnoreCase(f)))
      case (false, ExcludeFields(excludeFields)) =>
        row.filter(rowCell => !excludeFields.exists(f => rowCell.name.toLowerCase().equalsIgnoreCase(f)))
      case _ => row

  /** Applies field filter to the ArcaneSchema instance.
    * @param schema
    *   The data to filter.
    * @return
    *   The filtered data/schema.
    */
  def filter(schema: ArcaneSchema): ArcaneSchema = (fieldSelectionRule.isServerSide, fieldSelectionRule.rule) match
    case (false, IncludeFields(includeFields)) =>
      schema.filter(field => includeFields.exists(f => field.name.toLowerCase().equalsIgnoreCase(f)))
    case (false, ExcludeFields(excludeFields)) =>
      schema.filter(field => !excludeFields.exists(f => field.name.toLowerCase().equalsIgnoreCase(f)))
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
      case IncludeFields(includeFields) => fieldSelectionRule.essentialFields.subsetOf(includeFields)
      case ExcludeFields(excludeFields) => excludeFields.intersect(fieldSelectionRule.essentialFields).isEmpty
      case _                            => true
