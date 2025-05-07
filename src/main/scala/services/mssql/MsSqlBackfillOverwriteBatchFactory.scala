package com.sneaksanddata.arcane.framework
package services.mssql

import models.batches.{SqlServerChangeTrackingBackfillBatch, StagedBackfillOverwriteBatch}
import models.settings.{BackfillSettings, TablePropertiesSettings, TargetTableSettings}
import services.merging.JdbcMergeServiceClient
import com.sneaksanddata.arcane.framework.services.streaming.base.BackfillOverwriteBatchFactory

import zio.{Task, ZIO, ZLayer}

/** A factory that creates a backfill batch for the SQL Server data source.
  * @param jdbcMergeServiceClient
  *   The JDBC merge service client.
  * @param backfillSettings
  *   The backfill settings.
  * @param targetTableSettings
  *   The target table settings.
  * @param tablePropertiesSettings
  *   The table properties settings.
  */
class MsSqlBackfillOverwriteBatchFactory(
    jdbcMergeServiceClient: JdbcMergeServiceClient,
    backfillSettings: BackfillSettings,
    targetTableSettings: TargetTableSettings,
    tablePropertiesSettings: TablePropertiesSettings
) extends BackfillOverwriteBatchFactory:

  /** @inheritdoc
    */
  def createBackfillBatch: Task[StagedBackfillOverwriteBatch] =
    for schema <- jdbcMergeServiceClient.getSchema(backfillSettings.backfillTableFullName)
    yield SqlServerChangeTrackingBackfillBatch(
      backfillSettings.backfillTableFullName,
      schema,
      targetTableSettings.targetTableFullName,
      "",
      tablePropertiesSettings
    )

/** The companion object for the MsSqlBackfillBatchFactory class.
  */
object MsSqlBackfillOverwriteBatchFactory:

  /** The environment required for the MsSqlBackfillBatchFactory.
    */
  type Environment = JdbcMergeServiceClient & BackfillSettings & TargetTableSettings & TablePropertiesSettings

  /** Creates a new MsSqlBackfillBatchFactory.
    * @param jdbcMergeServiceClient
    *   The JDBC merge service client.
    * @param backfillSettings
    *   The backfill settings.
    * @param targetTableSettings
    *   The target table settings.
    * @param tablePropertiesSettings
    *   The table properties settings.
    * @return
    *   The MsSqlBackfillBatchFactory instance.
    */
  def apply(
      jdbcMergeServiceClient: JdbcMergeServiceClient,
      backfillSettings: BackfillSettings,
      targetTableSettings: TargetTableSettings,
      tablePropertiesSettings: TablePropertiesSettings
  ): MsSqlBackfillOverwriteBatchFactory =
    new MsSqlBackfillOverwriteBatchFactory(
      jdbcMergeServiceClient,
      backfillSettings,
      targetTableSettings,
      tablePropertiesSettings
    )

  /** The ZLayer for the MsSqlBackfillBatchFactory.
    */
  val layer: ZLayer[Environment, Nothing, BackfillOverwriteBatchFactory] =
    ZLayer {
      for
        mergeServiceClient      <- ZIO.service[JdbcMergeServiceClient]
        backfillSettings        <- ZIO.service[BackfillSettings]
        targetTableSettings     <- ZIO.service[TargetTableSettings]
        tablePropertiesSettings <- ZIO.service[TablePropertiesSettings]
      yield MsSqlBackfillOverwriteBatchFactory(
        mergeServiceClient,
        backfillSettings,
        targetTableSettings,
        tablePropertiesSettings
      )
    }
