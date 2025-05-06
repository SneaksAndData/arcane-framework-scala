package com.sneaksanddata.arcane.framework
package tests.services.filters

import models.*
import models.ArcaneType.{LongType, StringType}
import models.settings.{FieldSelectionRule, FieldSelectionRuleSettings}
import services.filters.FieldsFilteringService

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FieldsFilteringServiceTests extends AnyFlatSpec with Matchers:

  it should "be able to filter excluded fields from DataRow" in {
    val dataRows: DataRow = List(
      DataCell("colA", StringType, "valueA"),
      DataCell("colB", StringType, "valueB"),
      DataCell("Id", LongType, 1L),
      DataCell("versionnumber", LongType, 1L)
    )
    
    val fieldSelectionRule = FieldSelectionRule.ExcludeFields(Set("colA", "colB"))
    val settings = new FieldSelectionRuleSettings:
      override val rule: FieldSelectionRule = fieldSelectionRule
      override val essentialFields: Set[String] = Set("Id", "versionnumber")
      
    val fieldsFilteringService = FieldsFilteringService(settings)
    val filteredDataRows = fieldsFilteringService.filter(dataRows)
    
    filteredDataRows should not contain DataCell("colA", StringType, "valueA")
  }
  
  it should "be able to filter included fields from DataRow" in {
    val dataRows: DataRow = List(
      DataCell("colA", StringType, "valueA"),
      DataCell("colB", StringType, "valueB"),
      DataCell("Id", LongType, 1L),
      DataCell("versionnumber", LongType, 1L)
    )

    val fieldSelectionRule = FieldSelectionRule.IncludeFields(Set("colA", "colB", "Id", "versionnumber"))
    val settings = new FieldSelectionRuleSettings:
      override val rule: FieldSelectionRule = fieldSelectionRule
      override val essentialFields: Set[String] = Set("Id", "versionnumber")

    val fieldsFilteringService = FieldsFilteringService(settings)
    val filteredDataRows = fieldsFilteringService.filter(dataRows)

    filteredDataRows should not contain DataCell("Id", StringType, 1L)
  }

  it should "be able to filter excluded fields from ArcaneSchema" in {
    val schema: ArcaneSchema = List(
      Field("colA", StringType),
      Field("colB", StringType),
      Field("Id", LongType),
      Field("versionnumber", LongType)
    )

    val fieldSelectionRule = FieldSelectionRule.ExcludeFields(Set("colA", "colB"))
    val settings = new FieldSelectionRuleSettings:
      override val rule: FieldSelectionRule = fieldSelectionRule
      override val essentialFields: Set[String] = Set("Id", "versionnumber")

    val fieldsFilteringService = FieldsFilteringService(settings)
    val filteredDataRows = fieldsFilteringService.filter(schema)

    filteredDataRows should not contain Field("colA", StringType)
  }

  it should "be able to filter included fields from ArcaneSchema" in {
    val schema: ArcaneSchema = List(
      Field("colA", StringType),
      Field("colB", StringType),
      Field("Id", LongType),
      Field("versionnumber", LongType)
    )

    val fieldSelectionRule = FieldSelectionRule.IncludeFields(Set("colA", "colB", "Id", "versionnumber"))
    val settings = new FieldSelectionRuleSettings:
      override val rule: FieldSelectionRule = fieldSelectionRule
      override val essentialFields: Set[String] = Set("Id", "versionnumber")

    val fieldsFilteringService = FieldsFilteringService(settings)
    val filteredDataRows = fieldsFilteringService.filter(schema)

    filteredDataRows should not contain Field("Id", StringType)
  }
