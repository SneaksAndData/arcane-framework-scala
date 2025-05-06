package com.sneaksanddata.arcane.framework
package tests.extensions

import com.sneaksanddata.arcane.framework.extensions.BufferedReaderExtensions.{readMultilineCsvLine, streamMultilineCsv}

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table
import zio.test.ZIOSpecDefault
import zio.{Chunk, Runtime, Scope, Unsafe, ZIO}
import zio.test.*
import zio.test.TestAspect.timeout

import java.io.{BufferedReader, StringReader}

object BufferedReaderExtensionsTests extends ZIOSpecDefault:
  
  override def spec: Spec[TestEnvironment & Scope, Any] = suite("BufferedReaderExtensions") (
    Map(
      "1,2,3\n" -> Some("1,2,3\n"),
      "1,2,3" -> Some("1,2,3\n"),
      "1,2,\"3\"\n" -> Some("1,2,\"3\"\n"),
      "1,2,\"3\n4,5,6\n7,8,9\"" -> Some("1,2,\"3\n\n4,5,6\n7,8,9\""),
      "1,2,\"3\n4,5,6\n7,8,9\"\n\n\n" -> Some("1,2,\"3\n\n4,5,6\n7,8,9\""),
      "" -> None,
      "\n\n\n" -> None,
    ).map {
      case (line, expected) => test("read multiline CSV line correctly") {
        for 
          reader <- ZIO.succeed(new BufferedReader(new StringReader(line)))
          result <- reader.readMultilineCsvLine
        yield assertTrue(result == expected)
      }
    }.toSeq ++ 
      Seq(
        test("read the multiline CSV from file") {
          for
            multilineCSV <- ZIO.succeed("""1,2,3, "some text in quotes
                                          |that spans multiple lines. Also has a comma, but it's in quotes. And a newline\n"
                                          |4,5,6
                                          |""".stripMargin)
            reader <- ZIO.succeed(new BufferedReader(new StringReader(multilineCSV)))
            result <- reader.streamMultilineCsv.runCollect
          yield assertTrue(result == Chunk("1,2,3, \"some text in quotes\n\nthat spans multiple lines. Also has a comma, but it's in quotes. And a newline\\n\"", "4,5,6\n"))
        })
  ) @@ timeout(zio.Duration.fromSeconds(10)) @@ TestAspect.withLiveClock
