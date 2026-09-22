package com.sneaksanddata.arcane.framework
package tests.settings

import models.settings.sources.modification.{
  DefaultDataRowModificationSettings,
  ExcludeFieldSelector,
  FieldSelectorSetting,
  IncludeFieldSelector,
  SupportedModifications,
  SurrogateTimestamp,
  SurrogateTimestampImpl,
  SurrogateTimestampSetting
}

import org.scalatest.Inspectors.forAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should
import upickle.default.*

class DataRowModificationSettingsTests extends AnyFlatSpec with Matchers:

  private val testCases = List(
    (
      DefaultDataRowModificationSettings(SupportedModifications(None, None)),
      """{"modifications":[]}"""
    ),
    (
      DefaultDataRowModificationSettings(
        modificationSettings = SupportedModifications(
          fieldSelector = Some(
            FieldSelectorSetting(
              include = Some(IncludeFieldSelector(Seq("id", "name").toSet)),
              exclude = Some(ExcludeFieldSelector(Seq("secret").toSet))
            )
          ),
          surrogateTimestamp = Some(SurrogateTimestampSetting())
        )
      ),
      """{"modifications":[{"surrogateTimestamp":{}},{"fieldSelector":{"include":{"fields":["id","name"]},"exclude":{"fields":{["secret"]}}}]}"""
    )
  )

  it should "serialize correctly" in {
    forAll(testCases) { (settings, expected) =>
      write(settings) should equal(expected)
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
