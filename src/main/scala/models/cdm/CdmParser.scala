package com.sneaksanddata.arcane.framework
package models.cdm

import models.{ArcaneSchema, DataCell, DataRow, MergeKeyField}

import scala.language.implicitConversions
import scala.util.matching.Regex

object CSVParser:
  def parseCsvLine(line: String, delimiter: Char = ',', headerCount: Int): Seq[Option[String]] = {
    def isQuote(position: Int): Boolean = line(position) == '"'
    def isDelimiter(position: Int): Boolean = line(position) == delimiter
    def isEol(position: Int): Boolean = position == line.length - 1

    def extractValue(fromIndex: Int, currentPosition: Int): Option[String] =
      if currentPosition == fromIndex then
        if isDelimiter(fromIndex) then
          None
        else Some(line(currentPosition).toString)
      else
        (isDelimiter(currentPosition), isEol(currentPosition)) match
          // empty value at the end of the line
          case (true, true) if isQuote(currentPosition - 1) => Some(line.slice(from = fromIndex, until = currentPosition - 1)) // if there is a quote at the end, move back by 1 character
          case (true, true) => Some(line.slice(from = fromIndex, until = currentPosition))
          case (false, true) if !isQuote(currentPosition) => Some(line.slice(from = fromIndex, until = currentPosition + 1))
          case (false, true) if isQuote(currentPosition - 1) => Some(line.slice(from = fromIndex, until = currentPosition - 1))
          case (true, false) if isQuote(currentPosition - 1) => Some(line.slice(from = fromIndex, until = currentPosition - 1))
          case _ => Some(line.slice(from = fromIndex, until = currentPosition))

    val parsed = line.zipWithIndex.foldLeft((IndexedSeq[Option[String]](), 0, 0)) { (agg, element) =>
      val (character, charIndex) = element
      val (result, quoteSum, prevCharIndex) = agg

      def handleEol: (IndexedSeq[Option[String]], Int, Int) =
        if isQuote(prevCharIndex) then
          (result :+ extractValue(prevCharIndex + 1, charIndex), quoteSum, prevCharIndex)
        else
          (result :+ extractValue(prevCharIndex, charIndex), quoteSum, prevCharIndex)

      character match
        // recursive case in a quoted line - opening quote - move on
        case '"' if !isEol(charIndex) && quoteSum == 0 =>
          (result, quoteSum + 1, prevCharIndex)

        // recursive case in a quoted line - closing quote - move on
        case '"' if charIndex < line.length - 1 =>
          (result, quoteSum - 1, prevCharIndex)

        // EOL on quote
        case '"' if isEol(charIndex) => handleEol

        // hit a delimiter, not end of string - emit value and continue
        case _ if (quoteSum == 0) && isDelimiter(charIndex) && !isEol(charIndex) =>
          if isQuote(prevCharIndex) then // move ahead 1 character in case we hit a quote on a previous character
            (result :+ extractValue(prevCharIndex + 1, charIndex), quoteSum, charIndex + 1)
          else
            (result :+ extractValue(prevCharIndex, charIndex), quoteSum, charIndex + 1)

        case _ if (quoteSum == 0) && isDelimiter(charIndex) && isEol(charIndex) =>
          if isQuote(prevCharIndex) then
            (result :+ extractValue(prevCharIndex + 1, charIndex) :+ None, quoteSum, prevCharIndex)
          else
            (result :+ extractValue(prevCharIndex, charIndex) :+ None, quoteSum, prevCharIndex)

        // regular case - end of line - return last segment and exit
        case _ if (quoteSum == 0) && isEol(charIndex) => handleEol

        // mismatched quotes
        case _ if (quoteSum != 0) && isEol(charIndex) && !isQuote(charIndex) =>
          throw new IllegalStateException(s"CSV line $line with delimiter $delimiter has mismatching field quotes")
        case _ =>
          (result, quoteSum, prevCharIndex)
    }._1

    if parsed.size != headerCount then
      throw new IllegalStateException(s"CSV line $line with delimiter $delimiter cannot be parsed into desired $headerCount fields")

    parsed
  }

  def isComplete(csvLine: String): Boolean = {
    csvLine.count(_ == '"') % 2 == 0
  }

  def replaceQuotedNewlines(csvLine: String): String = {
    val regex = new Regex("\"[^\"]*(?:\"\"[^\"]*)*\"")
    regex.replaceSomeIn(csvLine, m => Some(m.matched.replace("\n", ""))).replace("\r", "")
  }


given Conversion[(String, ArcaneSchema), DataRow] with
  override def apply(schemaBoundCsvLine: (String, ArcaneSchema)): DataRow = schemaBoundCsvLine match
    case (csvLine, schema) =>
      val parsed = CSVParser.parseCsvLine(
        line = csvLine,
        headerCount = schema.size - SimpleCdmModel.systemFieldCount)
      
      val mergeKeyValue = parsed(schema.zipWithIndex.find(v => v._1.name == "Id").get._2)

      parsed
        .zipWithIndex
        .map { (fieldValue, index) =>
          val field = schema(index)
          DataCell(name = field.name, Type = field.fieldType, value = fieldValue)
        }.concat(
          Seq(DataCell(name = MergeKeyField.name, Type = MergeKeyField.fieldType, value = mergeKeyValue))
        ).toList
