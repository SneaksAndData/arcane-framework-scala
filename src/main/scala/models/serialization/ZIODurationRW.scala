package com.sneaksanddata.arcane.framework
package models.serialization

import upickle.ReadWriter
import upickle.default.readwriter

object ZIODurationRW:
  implicit val rw: ReadWriter[zio.Duration] = readwriter[String].bimap[zio.Duration](
    duration => scala.concurrent.duration.Duration.fromNanos(duration.toNanos).toString(),
    str => zio.Duration.fromNanos(scala.concurrent.duration.Duration(str).toNanos)
  )
