package com.sneaksanddata.arcane.framework
package models.serialization

import upickle.ReadWriter
import upickle.default.readwriter

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

object OffsetDateTimeRW:
  private val formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME

  implicit val rw: ReadWriter[OffsetDateTime] = readwriter[String].bimap[OffsetDateTime](
    offsetDateTime => formatter.format(offsetDateTime),
    str => OffsetDateTime.parse(str, formatter)
  )
