package com.sneaksanddata.arcane.framework
package utils
import models.settings.{RetryPolicyType, RetrySettings, StagingDataSettings}

object TestStagingDataSettings extends StagingDataSettings:
  override val stagingTablePrefix = "staging_"
  override val retryPolicy: RetrySettings = new RetrySettings:
    override val policyType: RetryPolicyType = RetryPolicyType.None
    override val initialDurationSeconds: Int = 0
    override val retryCount: Int = 0
