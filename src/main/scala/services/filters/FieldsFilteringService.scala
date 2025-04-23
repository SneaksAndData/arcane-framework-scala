package com.sneaksanddata.arcane.framework
package services.filters

import models.settings.{FieldSelectionRule, FieldSelectionRuleSettings}
import models.{ArcaneSchema, DataCell, DataRow, NamedCell}

import zio.{ZIO, ZLayer}

/**
 * The service that filters the fields of a DataRow or ArcaneSchema.
 */
class FieldsFilteringService(fieldSelectionRule: FieldSelectionRuleSettings):

  require(fieldSelectionRule.rule match
    case included: FieldSelectionRule.IncludeFields => fieldSelectionRule.essentialFields.subsetOf(included.fields)
    case excluded: FieldSelectionRule.ExcludeFields => excluded.fields.intersect(fieldSelectionRule.essentialFields).isEmpty
    case _ => true,

    "The field selection rule must not exclude essential fields: " + fieldSelectionRule.essentialFields.mkString(", ")
  )

  /**
   * Filters the fields of an ArcaneSchema.
   *
   * @param row The data to filter.
   * @return The filtered data/schema.
   */
  def filter(row: DataRow): DataRow = fieldSelectionRule.rule match
    case includeFields: FieldSelectionRule.IncludeFields => row.filter(entry => includeFields.fields.exists(f => entry.name.toLowerCase().equalsIgnoreCase(f)))
    case excludeFields: FieldSelectionRule.ExcludeFields => row.filter(entry => !excludeFields.fields.exists(f => entry.name.toLowerCase().equalsIgnoreCase(f)))
    case _ => row

  /**
   * Filters the fields of an ArcaneSchema.
   * @param row The data to filter.
   * @return The filtered data/schema.
   */
  def filter(row: ArcaneSchema): ArcaneSchema = fieldSelectionRule.rule match
    case includeFields: FieldSelectionRule.IncludeFields => row.filter(entry => includeFields.fields.exists(f => entry.name.toLowerCase().equalsIgnoreCase(f)))
    case excludeFields: FieldSelectionRule.ExcludeFields => row.filter(entry => !excludeFields.fields.exists(f => entry.name.toLowerCase().equalsIgnoreCase(f)))
    case _ => row

object FieldsFilteringService:
  type Environment = FieldSelectionRuleSettings

  def apply(fieldSelectionRule: FieldSelectionRuleSettings): FieldsFilteringService = new FieldsFilteringService(fieldSelectionRule)

  /**
   * The ZLayer that creates the IcebergConsumer.
   */
  val layer: ZLayer[Environment, Nothing, FieldsFilteringService] =
    ZLayer {
      for fieldSelectionRule <- ZIO.service[FieldSelectionRuleSettings]
        yield FieldsFilteringService(fieldSelectionRule)
    }
