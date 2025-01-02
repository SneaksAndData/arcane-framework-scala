package com.sneaksanddata.arcane.framework
package models

import models.cdm.CSVParser

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks.*


class CdmParserTests extends AnyFlatSpec with Matchers {

  private val validCsvLines = Table(
    ("line", "result"),
    ("\"qv1\",\"qv2\",\"qv3\",,\"qv4\",\"qv5\",\"qv6\",123,,0.12345", Seq(Some("qv1"), Some("qv2"), Some("qv3"), None, Some("qv4"), Some("qv5"), Some("qv6"), Some("123"), None, Some("0.12345"))),
    (",,123,341,5", Seq(None, None, Some("123"), Some("341"), Some("5"))),
    ("\"q\",,\"1321\"", Seq(Some("q"), None, Some("1321"))),
    ("\"q\",,\"13,21\"", Seq(Some("q"), None, Some("13,21"))),
    ("123,,\", abc def\"", Seq(Some("123"), None, Some(", abc def"))),
    ("5637144576,\"NFO\",,0,", Seq(Some("5637144576"), Some("NFO"), None, Some("0"), None))
  )

  private val invalidCsvlines = Table(
    ("line", "result"),
    ("\"q\",\",\"1321\"", Seq(Some("q"), None, Some("1321")))
  )

  it should "handle valid quoted CSV lines correctly" in {
    forAll (validCsvLines) { (line, result) =>
      CSVParser.parseCsvLine(line = line, headerCount = result.size) should equal(result)
    }
  }

  it should "handle invalid quoted CSV lines correctly" in {
    forAll (invalidCsvlines) { (line, result) =>
      intercept[IllegalStateException] {
        CSVParser.parseCsvLine(line, headerCount = result.size)
      }
    }
  }
}
