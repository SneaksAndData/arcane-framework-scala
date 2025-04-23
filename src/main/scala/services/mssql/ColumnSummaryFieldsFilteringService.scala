package com.sneaksanddata.arcane.framework
package services.mssql

import models.settings.{FieldSelectionRule, FieldSelectionRuleSettings}
import services.mssql.SqlDataCell.normalizeName
import services.mssql.base.MsSqlServerFieldsFilteringService

import scala.util.{Failure, Success, Try}


class ColumnSummaryFieldsFilteringService(fieldSelectionRule: FieldSelectionRuleSettings) extends MsSqlServerFieldsFilteringService:

  def filter(fields: List[ColumnSummary]): Try[List[ColumnSummary]] = fieldSelectionRule.rule match
    case includeFields: FieldSelectionRule.IncludeFields =>
      val groups = fields.groupBy { case (name, isPrimaryKey) => isPrimaryKey }
      val excludedPks = groups(true)
        .filter(entry => !includeFields.fields.exists(f => entry._1.normalizeName.toLowerCase().equalsIgnoreCase(f)))
        .map(_._1)
      
      excludedPks match
        case Nil => Success(fields.filter(entry => includeFields.fields.exists(f => entry._1.normalizeName.toLowerCase().equalsIgnoreCase(f))))
        case _ => Failure(new IllegalArgumentException(s"Fields ${toString(excludedPks)} are primary keys, and must be included in the field selection rule"))

    case excludeFields: FieldSelectionRule.ExcludeFields =>
      val groups = fields.groupBy { case (name, isPrimaryKey) => isPrimaryKey }
      val excludedPks = groups(true)
        .filter(entry => excludeFields.fields.exists(f => entry._1.normalizeName.toLowerCase().equalsIgnoreCase(f)))
        .map(_._1)

      excludedPks match
        case Nil => Success(fields.filter(entry => !excludeFields.fields.exists(f => entry._1.normalizeName.toLowerCase().equalsIgnoreCase(f))))
        case _ => Failure(new IllegalArgumentException(s"Fields ${toString(excludedPks)} are primary keys, and cannot be filtered out by the field selection rule"))

    case FieldSelectionRule.AllFields => Success(fields)

  private def toString(fields: List[String]) = "[" + fields.map(f => s"'$f'").mkString(", ") + "]"

