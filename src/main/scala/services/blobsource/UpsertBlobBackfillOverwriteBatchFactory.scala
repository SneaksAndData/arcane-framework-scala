package com.sneaksanddata.arcane.framework
package services.blobsource

import models.batches.{
  StagedBackfillOverwriteBatch,
  SynapseLinkBackfillOverwriteBatch,
  UpsertBlobBackfillOverwriteBatch
}
import models.settings.{BackfillSettings, TablePropertiesSettings, TargetTableSettings}
import services.merging.JdbcMergeServiceClient
import services.streaming.base.BackfillOverwriteBatchFactory

import zio.{Task, ZIO, ZLayer}

/** A factory that creates a backfill batch for the Blob Source.
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
class UpsertBlobBackfillOverwriteBatchFactory(
    jdbcMergeServiceClient: JdbcMergeServiceClient,
    backfillSettings: BackfillSettings,
    targetTableSettings: TargetTableSettings,
    tablePropertiesSettings: TablePropertiesSettings
) extends BackfillOverwriteBatchFactory:

  /** @inheritdoc
    */
  def createBackfillBatch: Task[StagedBackfillOverwriteBatch] =
    for schema <- jdbcMergeServiceClient.getSchema(backfillSettings.backfillTableFullName)
    yield UpsertBlobBackfillOverwriteBatch(
      backfillSettings.backfillTableFullName,
      schema,
      targetTableSettings.targetTableFullName,
      tablePropertiesSettings
    )

/** The companion object for the BlobSourceBackfillOverwriteBatchFactory class.
  */
object UpsertBlobBackfillOverwriteBatchFactory:

  /** The environment required for the BlobSourceBackfillOverwriteBatchFactory.
    */
  type Environment = JdbcMergeServiceClient & BackfillSettings & TargetTableSettings & TablePropertiesSettings

  /** Creates a new BlobSourceBackfillOverwriteBatchFactory.
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
  ): UpsertBlobBackfillOverwriteBatchFactory =
    new UpsertBlobBackfillOverwriteBatchFactory(
      jdbcMergeServiceClient,
      backfillSettings,
      targetTableSettings,
      tablePropertiesSettings
    )

  /** The ZLayer for the BlobSourceBackfillOverwriteBatchFactory.
    */
  val layer: ZLayer[Environment, Nothing, BackfillOverwriteBatchFactory] =
    ZLayer {
      for
        mergeServiceClient      <- ZIO.service[JdbcMergeServiceClient]
        backfillSettings        <- ZIO.service[BackfillSettings]
        targetTableSettings     <- ZIO.service[TargetTableSettings]
        tablePropertiesSettings <- ZIO.service[TablePropertiesSettings]
      yield UpsertBlobBackfillOverwriteBatchFactory(
        mergeServiceClient,
        backfillSettings,
        targetTableSettings,
        tablePropertiesSettings
      )
    }
