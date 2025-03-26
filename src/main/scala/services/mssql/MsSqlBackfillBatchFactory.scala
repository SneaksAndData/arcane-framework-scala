package com.sneaksanddata.arcane.framework
package services.mssql

import models.settings.{BackfillSettings, TablePropertiesSettings, TargetTableSettings}
import services.consumers.{SqlServerChangeTrackingBackfillBatch, StagedBackfillOverwriteBatch}
import services.merging.JdbcMergeServiceClient
import services.streaming.data_providers.backfill.BackfillBatchFactory

import zio.{Task, ZIO, ZLayer}

/**
 * A factory that creates a backfill batch for the SQL Server data source.
 * @param jdbcMergeServiceClient The JDBC merge service client.
 * @param backfillSettings The backfill settings.
 * @param targetTableSettings The target table settings.
 * @param tablePropertiesSettings The table properties settings.
 */
class MsSqlBackfillBatchFactory(jdbcMergeServiceClient: JdbcMergeServiceClient,
                                backfillSettings: BackfillSettings,
                                targetTableSettings: TargetTableSettings,
                                tablePropertiesSettings: TablePropertiesSettings)
  extends BackfillBatchFactory:

  /**
   @inheritdoc
   */
  def createBackfillBatch: Task[StagedBackfillOverwriteBatch] =
      for schema <- jdbcMergeServiceClient.getSchema(backfillSettings.backfillTableFullName)
        yield SqlServerChangeTrackingBackfillBatch(backfillSettings.backfillTableFullName,
          schema,
          targetTableSettings.targetTableFullName,
          "",
          tablePropertiesSettings)

/**
 * The companion object for the MsSqlBackfillBatchFactory class.
 */
object MsSqlBackfillBatchFactory:

  /**
   * The environment required for the MsSqlBackfillBatchFactory.
   */
  type Environment = JdbcMergeServiceClient
    & BackfillSettings
    & TargetTableSettings
    & TablePropertiesSettings

  /**
   * Creates a new MsSqlBackfillBatchFactory.
   * @param jdbcMergeServiceClient The JDBC merge service client.
   * @param backfillSettings The backfill settings.
   * @param targetTableSettings The target table settings.
   * @param tablePropertiesSettings The table properties settings.
   * @return The MsSqlBackfillBatchFactory instance.
   */
  def apply(jdbcMergeServiceClient: JdbcMergeServiceClient,
            backfillSettings: BackfillSettings,
            targetTableSettings: TargetTableSettings,
            tablePropertiesSettings: TablePropertiesSettings): MsSqlBackfillBatchFactory =
    new MsSqlBackfillBatchFactory(jdbcMergeServiceClient, backfillSettings, targetTableSettings, tablePropertiesSettings)


  /**
   * The ZLayer for the MsSqlBackfillBatchFactory.
   */
  val layer: ZLayer[Environment, Nothing, BackfillBatchFactory] =
    ZLayer {
      for
        streamDataProvider <- ZIO.service[JdbcMergeServiceClient]
        fieldFilteringProcessor <- ZIO.service[BackfillSettings]
        groupTransformer <- ZIO.service[TargetTableSettings]
        stagingProcessor <- ZIO.service[TablePropertiesSettings]
      yield MsSqlBackfillBatchFactory(streamDataProvider, fieldFilteringProcessor, groupTransformer, stagingProcessor)
    }
