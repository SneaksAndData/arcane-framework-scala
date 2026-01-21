package com.sneaksanddata.arcane.framework
package services.synapse

import models.batches.{StagedBackfillOverwriteBatch, SynapseLinkBackfillOverwriteBatch}
import models.settings.{BackfillSettings, TablePropertiesSettings, TargetTableSettings}
import services.merging.JdbcMergeServiceClient
import services.streaming.base.BackfillOverwriteBatchFactory

import zio.{Task, ZIO, ZLayer}

/** A factory that creates a backfill batch for the SQL Server data source.
  *
  * @param jdbcMergeServiceClient
  *   The JDBC merge service client.
  * @param backfillSettings
  *   The backfill settings.
  * @param targetTableSettings
  *   The target table settings.
  * @param tablePropertiesSettings
  *   The table properties settings.
  */
class SynapseBackfillOverwriteBatchFactory(
    jdbcMergeServiceClient: JdbcMergeServiceClient,
    backfillSettings: BackfillSettings,
    targetTableSettings: TargetTableSettings,
    tablePropertiesSettings: TablePropertiesSettings
) extends BackfillOverwriteBatchFactory:

  /** @inheritdoc
    */
  def createBackfillBatch: Task[StagedBackfillOverwriteBatch] =
    for schema <- jdbcMergeServiceClient.getSchema(backfillSettings.backfillTableFullName)
    yield SynapseLinkBackfillOverwriteBatch(
      backfillSettings.backfillTableFullName,
      schema,
      targetTableSettings.targetTableFullName,
      tablePropertiesSettings,
      None // TODO: requires watermark read
    )

/** The companion object for the SynapseBackfillOverwriteBatchFactory class.
  */
object SynapseBackfillOverwriteBatchFactory:

  /** The environment required for the SynapseBackfillOverwriteBatchFactory.
    */
  type Environment = JdbcMergeServiceClient & BackfillSettings & TargetTableSettings & TablePropertiesSettings

  /** Creates a new SynapseBackfillOverwriteBatchFactory.
    *
    * @param jdbcMergeServiceClient
    *   The JDBC merge service client.
    * @param backfillSettings
    *   The backfill settings.
    * @param targetTableSettings
    *   The target table settings.
    * @param tablePropertiesSettings
    *   The table properties settings.
    * @return
    *   The SynapseBackfillOverwriteBatchFactory instance.
    */
  def apply(
      jdbcMergeServiceClient: JdbcMergeServiceClient,
      backfillSettings: BackfillSettings,
      targetTableSettings: TargetTableSettings,
      tablePropertiesSettings: TablePropertiesSettings
  ): SynapseBackfillOverwriteBatchFactory =
    new SynapseBackfillOverwriteBatchFactory(
      jdbcMergeServiceClient,
      backfillSettings,
      targetTableSettings,
      tablePropertiesSettings
    )

  /** The ZLayer for the SynapseBackfillOverwriteBatchFactory.
    */
  val layer: ZLayer[Environment, Nothing, BackfillOverwriteBatchFactory] =
    ZLayer {
      for
        mergeServiceClient      <- ZIO.service[JdbcMergeServiceClient]
        backfillSettings        <- ZIO.service[BackfillSettings]
        targetTableSettings     <- ZIO.service[TargetTableSettings]
        tablePropertiesSettings <- ZIO.service[TablePropertiesSettings]
      yield SynapseBackfillOverwriteBatchFactory(
        mergeServiceClient,
        backfillSettings,
        targetTableSettings,
        tablePropertiesSettings
      )
    }
