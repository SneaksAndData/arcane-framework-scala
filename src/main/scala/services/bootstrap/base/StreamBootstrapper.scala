package com.sneaksanddata.arcane.framework
package services.bootstrap.base

import zio.Task

trait StreamBootstrapper:
  /** Cleans up streaming staging tables left after previous run. This method is used to ensure that the staging tables
    * are cleaned up after the streaming job restart. .
    */
  def cleanupStagingTables: Task[Unit]

  /** Creates the target table.
    *
    * @return
    *   The result of the target table creation operation.
    */
  def createTargetTable: Task[Unit]

  /** Creates the backfill staging table.
    *
    * @return
    *   The result of the backfill staging table creation operation.
    */
  def createBackFillTable: Task[Unit]

  /** Cleans up backfill tables from previous backfills: shard staging tables and shard combine table. This is a
    * safeguard in case backfill process doesn't complete the cleanup or is interrupted midway.
    * @return
    */
  def cleanupOutdatedBackfill: Task[Unit]
