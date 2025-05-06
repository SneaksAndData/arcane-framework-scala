package com.sneaksanddata.arcane.framework
package tests.extensions

import extensions.StringExtensions.camelCaseToSnakeCase

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks.*


class StringExtensionTests extends AnyFlatSpec with Matchers:

  private val testCases = Table(
    ("string", "expectedResult"),
    ("abc", "abc"),
    ("ABC", "abc"),
    ("aBc", "a_bc"),
    ("AbcdeFgh", "abcde_fgh"),
    ("_AbcdeFgh_", "_abcde_fgh_"),
    ("___", "___"),
    ("AaBbCcDd", "aa_bb_cc_dd"),
    ("MicrosoftSqlServerStream", "microsoft_sql_server_stream"),
    ("MicrosoftSynapseStream", "microsoft_synapse_stream"),
    ("Abcde-Fgh", "abcde-fgh"),
    ("A-_-bcde-Fgh", "a-_-bcde-fgh"),
    ("v0.1.2.3", "v0.1.2.3"),
  )

  it should "convert camel case to snakeCase" in {
    forAll (testCases) { (string, expected) =>
      val result = string.camelCaseToSnakeCase
      result should equal(expected)
    }
  }
