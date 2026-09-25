package com.sneaksanddata.arcane.framework
package models.settings.backfill

import models.serialization.OffsetDateTimeRW.*

import upickle.ReadWriter
import upickle.default.*

import java.time.OffsetDateTime

/** A partial override of `BackfillSettings` where every field is optional to support override/patch-style JSON
  * deserialization.
  */
trait OverrideBackfillSettings:
  val backfillStartDate: Option[OffsetDateTime]
  val backfillBehavior: Option[BackfillBehavior]

case class DefaultOverrideBackfillSettings(
    override val backfillBehavior: Option[BackfillBehavior] = None,
    override val backfillStartDate: Option[OffsetDateTime] = None
) extends OverrideBackfillSettings derives ReadWriter
