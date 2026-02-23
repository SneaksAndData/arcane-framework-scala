package com.sneaksanddata.arcane.framework
package tests.shared

import com.sneaksanddata.arcane.framework.models.settings.backfill.BackfillBehavior.Merge
import com.sneaksanddata.arcane.framework.models.settings.backfill.{BackfillBehavior, BackfillSettings}

import java.time.OffsetDateTime

object TestBackfillTableSettings extends BackfillSettings:
  override val backfillTableFullName: String             = "backfill_intermediate_table"
  override val backfillStartDate: Option[OffsetDateTime] = None
  override val backfillBehavior: BackfillBehavior        = Merge

class CustomTestBackfillTableSettings(override val backfillBehavior: BackfillBehavior) extends BackfillSettings:
  override val backfillTableFullName: String             = "test_full_name"
  override val backfillStartDate: Option[OffsetDateTime] = None
