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
      """{"supported":{}}"""
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
      """{"supported":{"fieldSelector":{"include":{"fields":["id","name"]}},"surrogateTimestamp":{}}}"""
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
      """{"supported":{"fieldSelector":{"exclude":{"fields":["description"]}},"surrogateTimestamp":{}}}"""
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

  it should "resolve empty rowModifications when no modifications are configured" in {
    val json     = """{"supported":{}}"""
    val settings = read[DefaultDataRowModificationSettings](json)
    settings.rowModifications should equal(Seq.empty)
  }

  it should "resolve rowModifications with include fieldSelector and surrogateTimestamp" in {
    val json =
      """{"supported":{"fieldSelector":{"include":{"fields":["id","name"]}},"surrogateTimestamp":{}}}"""
    val settings = read[DefaultDataRowModificationSettings](json)

    settings.rowModifications should equal(
      Seq(
        IncludeFieldSelectorImpl(IncludeFieldSelector(Set("id", "name"))),
        SurrogateTimestampImpl(SurrogateTimestamp())
      )
    )
  }

  it should "resolve rowModifications with exclude fieldSelector and surrogateTimestamp" in {
    val json =
      """{"supported":{"fieldSelector":{"exclude":{"fields":["description"]}},"surrogateTimestamp":{}}}"""
    val settings = read[DefaultDataRowModificationSettings](json)

    settings.rowModifications should equal(
      Seq(
        ExcludeFieldSelectorImpl(ExcludeFieldSelector(Set("description"))),
        SurrogateTimestampImpl(SurrogateTimestamp())
      )
    )
  }
