package com.sneaksanddata.arcane.framework
package models.settings.backfill

import java.time.OffsetDateTime

/** Backfill graph behavior. Merge will merge the backfill data with the existing data, while Overwrite will overwrite
  * the existing data with the backfill data.
  */
enum BackfillBehavior:
  case Merge, Overwrite

/** The settings that controls the backfill graph behavior
  */
trait BackfillSettings:

  /** The full name of the backfill table. It's the intermediate table that holds the backfill data. When backfill
    * process is completed, the data in this table will be merged or overwritten to the target table.
    */
  val backfillTableFullName: String

  /** The start date of the backfill process. If it's None, the backfill process will start from the beginning of the
    * data.
    */
  val backfillStartDate: Option[OffsetDateTime]

  /** The backfill behavior.
    */
  val backfillBehavior: BackfillBehavior
