package com.sneaksanddata.arcane.framework
package models

import models.cdm.CSVParser

import com.sneaksanddata.arcane.framework.models.cdm.CSVParser.replaceQuotedNewlines
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
    ("5637144576,\"NFO\",,0,", Seq(Some("5637144576"), Some("NFO"), None, Some("0"), None)),
    ("5637144576,\"$NFO\",,0,", Seq(Some("5637144576"), Some("$NFO"), None, Some("0"), None))
      ("000000d3-0000-0000-0000-005001000000,\"12/4/2024 4:44:32 PM\",\"12/4/2024 4:44:32 PM\",0,0,0,0,0,0,\"USA\",,,,,0,\"USA\",,0,,0,0,0,5637144576,0,,0,,,0,,\"2022-08-09T06:35:15.0000000Z\",\"2023-07-24T18:08:53.0000000Z\",,0,,,0,,0,0,,\"1900-01-01T00:00:00.0000000Z\",,,0,,\"2023-07-24T18:08:54.0000000Z\",\"RRAO\",0,\"2023-02-17T01:41:04.0000000Z\",\"?\",0,\"dat\",1071872623,5637144576,0,5637144576,211,0,\"2023-02-17T01:41:04.0000000+00:00\",\"2023-07-24T18:08:54.0000000Z\",", Seq())
  )

  private val invalidCsvlines = Table(
    ("line", "result"),
    ("\"q\",\",\"1321\"", Seq(Some("q"), None, Some("1321")))
  )

  it should "handle valid quoted CSV lines correctly" in {
    forAll (validCsvLines) { (line, result) =>
      val parseResult = CSVParser.parseCsvLine(line = line, headerCount = 62)
      parseResult should equal(result)
    }
  }
  
  it should "handle invalid quoted CSV lines correctly" in {
    forAll (invalidCsvlines) { (line, result) =>
      intercept[IllegalStateException] {
        CSVParser.parseCsvLine(line, headerCount = result.size)
      }
    }
  }

  it should "replace quoted newlines correctly " in {
    forAll (validCsvLines) { (line, result) =>
      replaceQuotedNewlines(line) should equal(line)
    }
  }

}
