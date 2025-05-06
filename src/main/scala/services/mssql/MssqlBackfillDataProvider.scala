package com.sneaksanddata.arcane.framework
package services.mssql

import models.DataRow

import zio.stream.ZStream

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
  def requestBackfill: ZStream[Any, Throwable, DataRow]
