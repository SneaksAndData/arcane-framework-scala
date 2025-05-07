package com.sneaksanddata.arcane.framework
package services.filters

import models.settings.{FieldSelectionRule, FieldSelectionRuleSettings}
import services.mssql.ColumnSummary
import services.mssql.SqlDataCell.normalizeName
import services.mssql.base.MsSqlServerFieldsFilteringService

import zio.{ZIO, ZLayer}

import scala.util.{Failure, Success, Try}

/** A service that filters the fields of a list of ColumnSummary based on the provided field selection rule.
  *
  * @param fieldSelectionRule
  *   The field selection rule to use.
  */
class ColumnSummaryFieldsFilteringService(fieldSelectionRule: FieldSelectionRuleSettings)
    extends MsSqlServerFieldsFilteringService:

  /** @inheritdoc
    */
  def filter(fields: List[ColumnSummary]): Try[List[ColumnSummary]] = fieldSelectionRule.rule match
    case includeFields: FieldSelectionRule.IncludeFields =>
      val groups = fields.groupBy { case (name, isPrimaryKey) => isPrimaryKey }
      val excludedPks = groups(true)
        .filter(entry => !includeFields.fields.exists(f => entry._1.normalizeName.toLowerCase().equalsIgnoreCase(f)))
        .map(_._1)

      excludedPks match
        case Nil =>
          Success(
            fields.filter(entry =>
              includeFields.fields.exists(f => entry._1.normalizeName.toLowerCase().equalsIgnoreCase(f))
            )
          )
        case _ =>
          Failure(
            new IllegalArgumentException(
              s"Fields ${toString(excludedPks)} are primary keys, and must be included in the field selection rule"
            )
          )

    case excludeFields: FieldSelectionRule.ExcludeFields =>
      val groups = fields.groupBy { case (name, isPrimaryKey) => isPrimaryKey }
      val excludedPks = groups(true)
        .filter(entry => excludeFields.fields.exists(f => entry._1.normalizeName.toLowerCase().equalsIgnoreCase(f)))
        .map(_._1)

      excludedPks match
        case Nil =>
          Success(
            fields.filter(entry =>
              !excludeFields.fields.exists(f => entry._1.normalizeName.toLowerCase().equalsIgnoreCase(f))
            )
          )
        case _ =>
          Failure(
            new IllegalArgumentException(
              s"Fields ${toString(excludedPks)} are primary keys, and cannot be filtered out by the field selection rule"
            )
          )

    case FieldSelectionRule.AllFields => Success(fields)

  private def toString(fields: List[String]) = "[" + fields.map(f => s"'$f'").mkString(", ") + "]"

object ColumnSummaryFieldsFilteringService:
  /** The environment for the ColumnSummaryFieldsFilteringService.
    */
  type Environment = FieldSelectionRuleSettings

  /** Creates a new ColumnSummaryFieldsFilteringService.
    *
    * @param fieldSelectionRule
    *   The field selection rule to use.
    * @return
    *   A new ColumnSummaryFieldsFilteringService.
    */
  def apply(fieldSelectionRule: FieldSelectionRuleSettings): ColumnSummaryFieldsFilteringService =
    new ColumnSummaryFieldsFilteringService(fieldSelectionRule)

  /** The ZLayer that creates the IcebergConsumer.
    */
  val layer: ZLayer[Environment, Nothing, ColumnSummaryFieldsFilteringService] =
    ZLayer {
      for fieldSelectionRule <- ZIO.service[FieldSelectionRuleSettings]
      yield ColumnSummaryFieldsFilteringService(fieldSelectionRule)
    }
