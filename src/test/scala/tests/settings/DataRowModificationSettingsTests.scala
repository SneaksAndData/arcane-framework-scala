package com.sneaksanddata.arcane.framework
package tests.settings

import models.settings.sources.modification.*

import org.scalatest.Inspectors.forAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should
import upickle.default.*

class DataRowModificationSettingsTests extends AnyFlatSpec with Matchers:

  private val testCases = List(
    (
      DefaultDataRowModificationSettings(SupportedModifications(None, None)),
      """{"modifications":{}}"""
    ),
    (
      DefaultDataRowModificationSettings(
        modificationSettings = SupportedModifications(
          fieldSelector = Some(
            FieldSelectorSetting(
              include = Some(IncludeFieldSelector(Seq("id", "name").toSet))
            )
          ),
          surrogateTimestamp = Some(SurrogateTimestampSetting())
        )
      ),
      """{"modifications":{"fieldSelector":{"include":{"fields":["id","name"]}},"surrogateTimestamp":{}}}"""
    ),
    (
      DefaultDataRowModificationSettings(
        modificationSettings = SupportedModifications(
          fieldSelector = Some(
            FieldSelectorSetting(
              exclude = Some(ExcludeFieldSelector(Seq("description").toSet))
            )
          ),
          surrogateTimestamp = Some(SurrogateTimestampSetting())
        )
      ),
      """{"modifications":{"fieldSelector":{"exclude":{"fields":["description"]}},"surrogateTimestamp":{}}}"""
    )
  )
  it should "serialize correctly" in {
    forAll(testCases) { (settings, expected) =>
      val y = write(settings)
      y should equal(expected)
    }
  }

  it should "deserialize correctly" in {
    forAll(testCases) { (settings, expected) =>
      read[DefaultDataRowModificationSettings](expected) should equal(settings)
    }
  }

//  it should "resolve modifications in their configured order" in {
//    val settings = testCases(1)._1
//
//    settings.modifications should equal(
//      Seq(
//        SurrogateTimestampImpl(SurrogateTimestamp()),
//        FieldSelectorImpl(
//          FieldSelectorSetting(
//            includeFields = Seq("id", "name"),
//            excludeFields = Seq("secret")
//          )
//        )
//      )
//    )
//  }
