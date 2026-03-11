package com.sneaksanddata.arcane.framework
package services.mssql

import models.app.PluginStreamContext
import models.batches.{SqlServerChangeTrackingBackfillBatch, StagedBackfillOverwriteBatch}
import models.settings.TableNaming.*
import models.settings.TablePropertiesSettings
import models.settings.sink.SinkSettings
import models.settings.staging.StagingTableSettings
import services.iceberg.base.StagingPropertyManager
import services.iceberg.given_Conversion_Schema_ArcaneSchema
import services.streaming.base.BackfillOverwriteBatchFactory

import zio.{Task, ZIO, ZLayer}

/** A factory that creates a backfill batch for the SQL Server data source.
  */
class MsSqlBackfillOverwriteBatchFactory(
    stagingPropertyManager: StagingPropertyManager,
    stagingTableSettings: StagingTableSettings,
    targetTableSettings: SinkSettings
) extends BackfillOverwriteBatchFactory:

  /** @inheritdoc
    */
  def createBackfillBatch(watermark: Option[String]): Task[StagedBackfillOverwriteBatch] =
    for schema <- stagingPropertyManager.getTableSchema(stagingTableSettings.backfillTableName.parts.name)
    yield SqlServerChangeTrackingBackfillBatch(
      stagingTableSettings.backfillTableName,
      schema,
      targetTableSettings.targetTableFullName,
      targetTableSettings.targetTableProperties,
      watermark
    )

/** The companion object for the MsSqlBackfillBatchFactory class.
  */
object MsSqlBackfillOverwriteBatchFactory:

  /** The environment required for the MsSqlBackfillBatchFactory.
    */
  private type Environment = StagingPropertyManager & PluginStreamContext

  /** Creates a new MsSqlBackfillBatchFactory.
    * @return
    *   The MsSqlBackfillBatchFactory instance.
    */
  def apply(
      stagingPropertyManager: StagingPropertyManager,
      stagingTableSettings: StagingTableSettings,
      targetTableSettings: SinkSettings
  ): MsSqlBackfillOverwriteBatchFactory =
    new MsSqlBackfillOverwriteBatchFactory(
      stagingPropertyManager,
      stagingTableSettings,
      targetTableSettings
    )

  /** The ZLayer for the MsSqlBackfillBatchFactory.
    */
  val layer: ZLayer[Environment, Nothing, BackfillOverwriteBatchFactory] =
    ZLayer {
      for
        stagingPropertyManager  <- ZIO.service[StagingPropertyManager]
        context                <- ZIO.service[PluginStreamContext]
      yield MsSqlBackfillOverwriteBatchFactory(
        stagingPropertyManager,
        context.staging.table,
        context.sink
      )
    }
