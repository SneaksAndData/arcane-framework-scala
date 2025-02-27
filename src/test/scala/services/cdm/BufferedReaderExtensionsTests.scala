package com.sneaksanddata.arcane.framework
package services.cdm

import services.cdm.BufferedReaderExtensions.readMultilineCsv

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table
import zio.{Runtime, Unsafe}

import java.io.{BufferedReader, StringReader}

class BufferedReaderExtensionsTests extends AsyncFlatSpec with Matchers:
  private val runtime = Runtime.default

  private val cases = Table(
    ("testcase", "expected"),
    ("1,2,3\n", Some("1,2,3\n")),
    ("1,2,3", Some("1,2,3\n")),
    ("1,2,\"3\"\n", Some("1,2,\"3\"\n")),
    ("1,2,\"3\n4,5,6\n7,8,9\"", Some("1,2,\"3\n\n4,5,6\n7,8,9\"")),
    ("1,2,\"3\n4,5,6\n7,8,9\"\n\n\n", Some("1,2,\"3\n\n4,5,6\n7,8,9\"")),
    ("", None),
    ("\n\n\n", None),
  )

  it should "read the lines form the file" in {
    forAll(cases) { (data, expected) =>
      val reader = new BufferedReader(new StringReader(data))
      Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(reader.readMultilineCsv)).map { result =>
        result should be(expected)
      }
    }
  }
