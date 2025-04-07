package com.sneaksanddata.arcane.framework
package services.mssql

import services.mssql.MsSqlConnection.BackfillBatch

import zio.Task

/**
 * A trait that represents a backfill data provider.
 * TODO: Deprecated - MSSQL Only
 */
trait MssqlBackfillDataProvider:

  /**
   * Provides the backfill data.
   *
   * @return A task that represents the backfill data.
   */
  def requestBackfill: Task[BackfillBatch]
