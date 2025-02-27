package com.sneaksanddata.arcane.framework
package services.cdm

import zio.ZIO

import java.io.BufferedReader

object BufferedReaderExtensions:

  /**
   * Reads a CSV file line by line, concatenating lines that are split by newlines within a quoted field.
   * @param stream The BufferedReader to read from.
   * @return A ZIO effect that represents the next line of the CSV file.
   */
  extension (stream: BufferedReader) def readMultilineCsv: ZIO[Any, Throwable, Option[String]] =
    for {
      dataLine <- ZIO.attemptBlocking(Option(stream.readLine()))
      continuation <- tryGetContinuation(stream, dataLine.getOrElse("").count(_ == '"'), new StringBuilder())
    }
    yield {
      dataLine match
        case None => None
        case Some(dataLine) if dataLine == "" => None
        case Some(dataLine) => Some(s"$dataLine\n$continuation")
    }

  private def tryGetContinuation(stream: BufferedReader, quotes: Int, accum: StringBuilder): ZIO[Any, Throwable, String] =
    if quotes % 2 == 0 then
      ZIO.succeed(accum.toString())
    else
      for {
        line <- ZIO.attemptBlocking(Option(stream.readLine()))
        continuation <- tryGetContinuation(stream, quotes + line.getOrElse("").count(_ == '"'), accum.append(line.map(l => s"\n$l").getOrElse("")))
      }
      yield continuation
