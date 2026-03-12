package com.sneaksanddata.arcane.framework
package tests.settings

import models.settings.{AllFields, DefaultFieldSelectionRuleSettings, FieldSelectionRuleSetting, IncludeFields}

import org.scalatest.Inspectors.forAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should
import upickle.default.*

class FieldSelectionRuleSettingsTests extends AnyFlatSpec with Matchers:

  private val testCases = List(
    (
      DefaultFieldSelectionRuleSettings(
        essentialFields = Set("colA"),
        ruleSetting = FieldSelectionRuleSetting(
          all = Some(AllFields()),
          include = None,
          exclude = None
        ),
        isServerSide = false
      ),
      """{"essentialFields":["colA"],"rule":{"all":{},"include":null,"exclude":null},"isServerSide":false}"""
    ),
    (
      DefaultFieldSelectionRuleSettings(
        essentialFields = Set("colA"),
        ruleSetting = FieldSelectionRuleSetting(
          all = None,
          include = Some(IncludeFields(Set("colB"))),
          exclude = None
        ),
        isServerSide = false
      ),
      """{"essentialFields":["colA"],"rule":{"all":null,"include":{"fields":["colB"]},"exclude":null},"isServerSide":false}"""
    ),
    (
      DefaultFieldSelectionRuleSettings(
        essentialFields = Set(),
        ruleSetting = FieldSelectionRuleSetting(
          all = None,
          include = None,
          exclude = None
        ),
        isServerSide = true
      ),
      """{"essentialFields":[],"rule":{"all":null,"include":null,"exclude":null},"isServerSide":true}"""
    )
  )

  it should "serialize correctly" in {
    forAll(testCases) { (settings, expected) =>
      upickle.write(settings) should equal(expected)
    }
  }

  it should "deserialize correctly" in {
    forAll(testCases) { (settings, expected) =>
      read[DefaultFieldSelectionRuleSettings](expected) should equal(settings)
    }
  }
