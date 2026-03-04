package com.sneaksanddata.arcane.framework
package services.synapse

import models.batches.{StagedBackfillOverwriteBatch, SynapseLinkBackfillOverwriteBatch}
import models.settings.TablePropertiesSettings
import models.settings.backfill.BackfillSettings
import models.settings.sink.SinkSettings
import services.iceberg.base.StagingPropertyManager
import services.streaming.base.BackfillOverwriteBatchFactory
import services.iceberg.given_Conversion_Schema_ArcaneSchema

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
    stagingTablePropertyManager: StagingPropertyManager,
    backfillSettings: BackfillSettings,
    targetTableSettings: SinkSettings,
    tablePropertiesSettings: TablePropertiesSettings
) extends BackfillOverwriteBatchFactory:

  /** @inheritdoc
    */
  def createBackfillBatch(watermark: Option[String]): Task[StagedBackfillOverwriteBatch] =
    for schema <- stagingTablePropertyManager.getTableSchema(backfillSettings.backfillTableNameParts.Name)
    yield SynapseLinkBackfillOverwriteBatch(
      backfillSettings.backfillTableFullName,
      schema,
      targetTableSettings.targetTableFullName,
      tablePropertiesSettings,
      watermark
    )

/** The companion object for the SynapseBackfillOverwriteBatchFactory class.
  */
object SynapseBackfillOverwriteBatchFactory:

  /** The environment required for the SynapseBackfillOverwriteBatchFactory.
    */
  private type Environment = StagingPropertyManager & BackfillSettings & SinkSettings & TablePropertiesSettings

  /** Creates a new SynapseBackfillOverwriteBatchFactory.
    *
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
      stagingPropertyManager: StagingPropertyManager,
      backfillSettings: BackfillSettings,
      targetTableSettings: SinkSettings,
      tablePropertiesSettings: TablePropertiesSettings
  ): SynapseBackfillOverwriteBatchFactory =
    new SynapseBackfillOverwriteBatchFactory(
      stagingPropertyManager,
      backfillSettings,
      targetTableSettings,
      tablePropertiesSettings
    )

  /** The ZLayer for the SynapseBackfillOverwriteBatchFactory.
    */
  val layer: ZLayer[Environment, Nothing, BackfillOverwriteBatchFactory] =
    ZLayer {
      for
        stagingPropertyManager  <- ZIO.service[StagingPropertyManager]
        backfillSettings        <- ZIO.service[BackfillSettings]
        targetTableSettings     <- ZIO.service[SinkSettings]
        tablePropertiesSettings <- ZIO.service[TablePropertiesSettings]
      yield SynapseBackfillOverwriteBatchFactory(
        stagingPropertyManager,
        backfillSettings,
        targetTableSettings,
        tablePropertiesSettings
      )
    }
