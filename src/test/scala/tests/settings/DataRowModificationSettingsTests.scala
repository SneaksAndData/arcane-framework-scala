package com.sneaksanddata.arcane.framework
package tests.settings

import models.settings.sources.modification.{
  DataRowModificationSetting,
  DefaultDataRowModificationSettings,
  FieldSelector,
  FieldSelectorImpl,
  SurrogateTimestamp,
  SurrogateTimestampImpl
}

import org.scalatest.Inspectors.forAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should
import upickle.default.*

class DataRowModificationSettingsTests extends AnyFlatSpec with Matchers:

  private val testCases = List(
    (
      DefaultDataRowModificationSettings(Seq.empty),
      """{"modifications":[]}"""
    ),
    (
      DefaultDataRowModificationSettings(
        Seq(
          DataRowModificationSetting(surrogateTimestamp = Some(SurrogateTimestamp())),
          DataRowModificationSetting(
            fieldSelector = Some(
              FieldSelector(
                includeFields = Seq("id", "name"),
                excludeFields = Seq("secret")
              )
            )
          )
        )
      ),
      """{"modifications":[{"surrogateTimestamp":{}},{"fieldSelector":{"includeFields":["id","name"],"excludeFields":["secret"]}}]}"""
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

  it should "resolve modifications in their configured order" in {
    val settings = testCases(1)._1

    settings.modifications should equal(
      Seq(
        SurrogateTimestampImpl(SurrogateTimestamp()),
        FieldSelectorImpl(
          FieldSelector(
            includeFields = Seq("id", "name"),
            excludeFields = Seq("secret")
          )
        )
      )
    )
  }

  it should "reject an empty modification entry" in {
    an[IllegalArgumentException] should be thrownBy {
      DataRowModificationSetting().resolveSetting
    }
  }

  it should "reject an entry containing multiple modifications" in {
    an[IllegalArgumentException] should be thrownBy {
      DataRowModificationSetting(
        surrogateTimestamp = Some(SurrogateTimestamp()),
        fieldSelector = Some(FieldSelector())
      ).resolveSetting
    }
  }
