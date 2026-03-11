package com.sneaksanddata.arcane.framework
package services.synapse

import models.app.PluginStreamContext
import models.batches.{StagedBackfillOverwriteBatch, SynapseLinkBackfillOverwriteBatch}
import models.settings.TableNaming.*
import models.settings.sink.SinkSettings
import models.settings.staging.StagingTableSettings
import services.iceberg.base.StagingPropertyManager
import services.iceberg.given_Conversion_Schema_ArcaneSchema
import services.streaming.base.BackfillOverwriteBatchFactory

import zio.{Task, ZIO, ZLayer}

/** A factory that creates a backfill batch for the SQL Server data source.
  */
class SynapseBackfillOverwriteBatchFactory(
    stagingTablePropertyManager: StagingPropertyManager,
    stagingTableSettings: StagingTableSettings,
    targetTableSettings: SinkSettings
) extends BackfillOverwriteBatchFactory:

  /** @inheritdoc
    */
  def createBackfillBatch(watermark: Option[String]): Task[StagedBackfillOverwriteBatch] =
    for schema <- stagingTablePropertyManager.getTableSchema(stagingTableSettings.backfillTableName.parts.name)
    yield SynapseLinkBackfillOverwriteBatch(
      stagingTableSettings.backfillTableName,
      schema,
      targetTableSettings.targetTableFullName,
      targetTableSettings.targetTableProperties,
      watermark
    )

/** The companion object for the SynapseBackfillOverwriteBatchFactory class.
  */
object SynapseBackfillOverwriteBatchFactory:

  /** The environment required for the SynapseBackfillOverwriteBatchFactory.
    */
  private type Environment = StagingPropertyManager & PluginStreamContext

  /** Creates a new SynapseBackfillOverwriteBatchFactory.
    * @return
    *   The SynapseBackfillOverwriteBatchFactory instance.
    */
  def apply(
      stagingPropertyManager: StagingPropertyManager,
      stagingTableSettings: StagingTableSettings,
      targetTableSettings: SinkSettings
  ): SynapseBackfillOverwriteBatchFactory =
    new SynapseBackfillOverwriteBatchFactory(
      stagingPropertyManager,
      stagingTableSettings,
      targetTableSettings
    )

  /** The ZLayer for the SynapseBackfillOverwriteBatchFactory.
    */
  val layer: ZLayer[Environment, Nothing, BackfillOverwriteBatchFactory] =
    ZLayer {
      for
        context <- ZIO.service[PluginStreamContext]
        stagingPropertyManager  <- ZIO.service[StagingPropertyManager]
      yield SynapseBackfillOverwriteBatchFactory(
        stagingPropertyManager,
        context.staging.table,
        context.sink
      )
    }
