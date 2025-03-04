package com.sneaksanddata.arcane.framework
package models.settings

import zio.Schedule

import java.time.Duration


enum RetryPolicyType:
  case ExponentialBackoff
  case None

trait RetrySettings:
  val policyType: RetryPolicyType

  val initialDurationSeconds: Int
  val retryCount: Int


object RetrySettings:
  type ScheduleType = Schedule.WithState[(Long, Long), Any, Any, (zio.Duration, Long)]
  
  extension (settings: RetrySettings)
    def toSchedule: Option[ScheduleType] = settings.policyType match
      case RetryPolicyType.ExponentialBackoff => Some(Schedule.exponential(Duration.ofSeconds(settings.initialDurationSeconds)) && Schedule.recurs(settings.initialDurationSeconds))
      case RetryPolicyType.None => None

