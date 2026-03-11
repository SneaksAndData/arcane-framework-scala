package com.sneaksanddata.arcane.framework
package models.settings.backfill

import models.serialization.OffsetDateTimeRW.*

import upickle.ReadWriter
import upickle.default.*

import java.time.OffsetDateTime

/** Backfill graph behavior. Merge will merge the backfill data with the existing data, while Overwrite will overwrite
  * the existing data with the backfill data.
  */
enum BackfillBehavior derives ReadWriter:
  case Merge, Overwrite

/** The settings that controls the backfill graph behavior
  */
trait BackfillSettings:

  /** The start date of the backfill process. If it's None, the backfill process will start from the beginning of the
    * data.
    */
  val backfillStartDate: Option[OffsetDateTime]

  /** The backfill behavior.
    */
  val backfillBehavior: BackfillBehavior

case class DefaultBackfillSettings(
    override val backfillBehavior: BackfillBehavior,
    override val backfillStartDate: Option[OffsetDateTime]
) extends BackfillSettings derives ReadWriter
