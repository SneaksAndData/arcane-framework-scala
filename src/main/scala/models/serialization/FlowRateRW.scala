package com.sneaksanddata.arcane.framework
package models.serialization

import models.settings.FlowRate

import upickle.ReadWriter
import upickle.default.readwriter

object FlowRateRW:
  /** Serialized format: {elements} per {period} Example: 100 per 15 second
    */
  implicit val rw: ReadWriter[FlowRate] = readwriter[String].bimap[FlowRate](
    rate => s"${rate.elements} per ${scala.concurrent.duration.Duration.fromNanos(rate.interval.toNanos).toString()}",
    str =>
      str.split(" per ").toList match
        case elementsStr :: periodStr :: nil =>
          FlowRate(
            elements = elementsStr.toInt,
            interval = zio.Duration.fromNanos(scala.concurrent.duration.Duration(periodStr).toNanos)
          )
        case _ => throw new RuntimeException("Invalid format for FlowRate, must be `{elements} per {period}`")
  )
