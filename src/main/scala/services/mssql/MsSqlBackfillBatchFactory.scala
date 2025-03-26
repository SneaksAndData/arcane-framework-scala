package com.sneaksanddata.arcane.framework
package services.mssql

import services.consumers.{SqlServerChangeTrackingBackfillBatch, StagedBackfillOverwriteBatch}

import com.sneaksanddata.arcane.framework.models.settings.{BackfillSettings, SinkSettings, TablePropertiesSettings, TargetTableSettings}
import com.sneaksanddata.arcane.framework.services.merging.JdbcMergeServiceClient
import com.sneaksanddata.arcane.framework.services.streaming.data_providers.backfill.BackfillBatchFactory
import zio.{Task, ZIO, ZLayer}

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

object MsSqlBackfillBatchFactory:
  type Environment = JdbcMergeServiceClient & BackfillSettings & TargetTableSettings & TablePropertiesSettings
  
  def apply(jdbcMergeServiceClient: JdbcMergeServiceClient,
            backfillSettings: BackfillSettings,
            targetTableSettings: TargetTableSettings,
            tablePropertiesSettings: TablePropertiesSettings): MsSqlBackfillBatchFactory =
    new MsSqlBackfillBatchFactory(jdbcMergeServiceClient, backfillSettings, targetTableSettings, tablePropertiesSettings)
    
    
  /**
   * The ZLayer for the GenericStreamingGraphBuilder.
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
