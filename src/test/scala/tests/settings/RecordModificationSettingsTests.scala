package com.sneaksanddata.arcane.framework
package tests.settings

import models.settings.sources.{
  DefaultRecordModificationSettings,
  FieldSelector,
  FieldSelectorImpl,
  LoadTimestamp,
  LoadTimestampImpl,
  RecordModificationSetting,
  SurrogateMergeKey,
  SurrogateMergeKeyImpl,
  SurrogateVersion,
  SurrogateVersionImpl
}

import org.scalatest.Inspectors.forAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should
import upickle.default.*

class RecordModificationSettingsTests extends AnyFlatSpec with Matchers:

  private val testCases = List(
    (
      DefaultRecordModificationSettings(Seq.empty),
      """{"modifications":[]}"""
    ),
    (
      DefaultRecordModificationSettings(
        Seq(
          RecordModificationSetting(surrogateMergeKey = Some(SurrogateMergeKey())),
          RecordModificationSetting(surrogateVersion = Some(SurrogateVersion())),
          RecordModificationSetting(loadTimestamp = Some(LoadTimestamp())),
          RecordModificationSetting(
            fieldSelector = Some(
              FieldSelector(
                includeFields = Seq("id", "name"),
                excludeFields = Seq("secret")
              )
            )
          )
        )
      ),
      """{"modifications":[{"surrogateMergeKey":{}},{"surrogateVersion":{}},{"loadTimestamp":{}},{"fieldSelector":{"includeFields":["id","name"],"excludeFields":["secret"]}}]}"""
    )
  )

  it should "serialize correctly" in {
    forAll(testCases) { (settings, expected) =>
      write(settings) should equal(expected)
    }
  }

  it should "deserialize correctly" in {
    forAll(testCases) { (settings, expected) =>
      read[DefaultRecordModificationSettings](expected) should equal(settings)
    }
  }

  it should "resolve modifications in their configured order" in {
    val settings = testCases(1)._1

    settings.modifications should equal(
      Seq(
        SurrogateMergeKeyImpl(SurrogateMergeKey()),
        SurrogateVersionImpl(SurrogateVersion()),
        LoadTimestampImpl(LoadTimestamp()),
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
      RecordModificationSetting().resolveSetting
    }
  }

  it should "reject an entry containing multiple modifications" in {
    an[IllegalArgumentException] should be thrownBy {
      RecordModificationSetting(
        surrogateMergeKey = Some(SurrogateMergeKey()),
        loadTimestamp = Some(LoadTimestamp())
      ).resolveSetting
    }
  }
