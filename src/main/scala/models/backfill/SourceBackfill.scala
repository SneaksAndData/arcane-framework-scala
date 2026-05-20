package com.sneaksanddata.arcane.framework
package models.backfill

import models.serialization.OffsetDateTimeRW.*

import upickle.ReadWriter

import java.time.OffsetDateTime

trait SourceBackfill:
  val id: String
  val startedAt: OffsetDateTime
  val completedAt: Option[OffsetDateTime]

  def isCompleted: Boolean = completedAt.isDefined

case class DefaultSourceBackfill(
    override val id: String,
    override val startedAt: OffsetDateTime,
    override val completedAt: Option[OffsetDateTime]
) extends SourceBackfill derives ReadWriter

object DefaultSourceBackfill:
  def apply(value: String): DefaultSourceBackfill  = upickle.read[DefaultSourceBackfill](value)
  def toJson(value: DefaultSourceBackfill): String = upickle.write(value)
