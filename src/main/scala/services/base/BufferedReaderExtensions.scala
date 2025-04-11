package com.sneaksanddata.arcane.framework
package services.base

import zio.ZIO
import zio.stream.ZStream

import java.io.BufferedReader

object BufferedReaderExtensions:


  /**
   * Reads a CSV file line by line, concatenating lines that are split by newlines within a quoted field.
   * The reader should be closed after the stream is consumed.
   * @return A ZStream effect that represents the next line of the CSV file.
   */
  extension (javaReader: BufferedReader) def streamMultilineCsv: ZStream[Any, Throwable, String] =
    ZStream.repeatZIO(javaReader.readMultilineCsvLine)
      .collectWhile { case Some(line) => line }

  /**
   * Reads a CSV file line by line, concatenating lines that are split by newlines within a quoted field.
   * @return A ZIO effect that represents the next line of the CSV file.
   */
  extension (reader: BufferedReader) def readMultilineCsvLine: ZIO[Any, Throwable, Option[String]] =
    for {
      dataLine <- ZIO.attemptBlocking(Option(reader.readLine()))
      continuation <- tryGetContinuation(reader, dataLine.getOrElse("").count(_ == '"'), new StringBuilder())
    }
    yield {
      dataLine match
        case None => None
        case Some(dataLine) if dataLine == "" => None
        case Some(dataLine) => Some(s"$dataLine\n$continuation")
    }

  private def tryGetContinuation(reader: BufferedReader, quotes: Int, accum: StringBuilder): ZIO[Any, Throwable, String] =
    if quotes % 2 == 0 then
      ZIO.succeed(accum.toString())
    else
      for {
        line <- ZIO.attemptBlocking(Option(reader.readLine()))
        continuation <- tryGetContinuation(reader, quotes + line.getOrElse("").count(_ == '"'), accum.append(line.map(l => s"\n$l").getOrElse("")))
      }
      yield continuation
