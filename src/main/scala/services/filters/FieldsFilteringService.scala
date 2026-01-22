package com.sneaksanddata.arcane.framework
package services.filters

import models.schemas.{ArcaneSchema, DataRow, JsonWatermarkRow}
import models.settings.{FieldSelectionRule, FieldSelectionRuleSettings}
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
      case (false, includeFields: FieldSelectionRule.IncludeFields) =>
        row.filter(rowCell => includeFields.fields.exists(f => rowCell.name.toLowerCase().equalsIgnoreCase(f)))
      case (false, excludeFields: FieldSelectionRule.ExcludeFields) =>
        row.filter(rowCell => !excludeFields.fields.exists(f => rowCell.name.toLowerCase().equalsIgnoreCase(f)))
      case _ => row

  /** Applies field filter to the ArcaneSchema instance.
    * @param schema
    *   The data to filter.
    * @return
    *   The filtered data/schema.
    */
  def filter(schema: ArcaneSchema): ArcaneSchema = (fieldSelectionRule.isServerSide, fieldSelectionRule.rule) match
    case (false, includeFields: FieldSelectionRule.IncludeFields) =>
      schema.filter(field => includeFields.fields.exists(f => field.name.toLowerCase().equalsIgnoreCase(f)))
    case (false, excludeFields: FieldSelectionRule.ExcludeFields) =>
      schema.filter(field => !excludeFields.fields.exists(f => field.name.toLowerCase().equalsIgnoreCase(f)))
    case _ => schema

object FieldsFilteringService:

  type Environment = FieldSelectionRuleSettings

  def apply(fieldSelectionRule: FieldSelectionRuleSettings): FieldsFilteringService = new FieldsFilteringService(
    fieldSelectionRule
  )

  /** The ZLayer that creates the IcebergConsumer.
    */
  val layer: ZLayer[Environment, Nothing, FieldsFilteringService] =
    ZLayer {
      for fieldSelectionRule <- ZIO.service[FieldSelectionRuleSettings]
      yield FieldsFilteringService(fieldSelectionRule)
    }

  /** Validates that the field selection rule does not exclude essential fields
    */
  extension (fieldSelectionRule: FieldSelectionRuleSettings)
    def isValid: Boolean = fieldSelectionRule.rule match
      case included: FieldSelectionRule.IncludeFields => fieldSelectionRule.essentialFields.subsetOf(included.fields)
      case excluded: FieldSelectionRule.ExcludeFields =>
        excluded.fields.intersect(fieldSelectionRule.essentialFields).isEmpty
      case _ => true
