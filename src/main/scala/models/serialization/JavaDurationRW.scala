package com.sneaksanddata.arcane.framework
package models.serialization

import upickle.ReadWriter
import upickle.default.readwriter

object JavaDurationRW:
  implicit val rw: ReadWriter[java.time.Duration] = readwriter[String].bimap[java.time.Duration](
    duration => scala.concurrent.duration.Duration.fromNanos(duration.toNanos).toString(),
    str => zio.Duration.fromNanos(scala.concurrent.duration.Duration(str).toNanos)
  )
