package com.sneaksanddata.arcane.framework
package services.mssql

import models.batches.{SqlServerChangeTrackingBackfillBatch, StagedBackfillOverwriteBatch}
import models.settings.TablePropertiesSettings
import models.settings.backfill.BackfillSettings
import models.settings.sink.SinkSettings
import services.iceberg.base.StagingPropertyManager
import services.streaming.base.BackfillOverwriteBatchFactory
import services.iceberg.given_Conversion_Schema_ArcaneSchema

import zio.{Task, ZIO, ZLayer}

/** A factory that creates a backfill batch for the SQL Server data source.
  * @param backfillSettings
  *   The backfill settings.
  * @param targetTableSettings
  *   The target table settings.
  * @param tablePropertiesSettings
  *   The table properties settings.
  */
class MsSqlBackfillOverwriteBatchFactory(
    stagingPropertyManager: StagingPropertyManager,
    backfillSettings: BackfillSettings,
    targetTableSettings: SinkSettings,
    tablePropertiesSettings: TablePropertiesSettings
) extends BackfillOverwriteBatchFactory:

  /** @inheritdoc
    */
  def createBackfillBatch(watermark: Option[String]): Task[StagedBackfillOverwriteBatch] =
    for schema <- stagingPropertyManager.getTableSchema(backfillSettings.backfillTableNameParts.Name)
    yield SqlServerChangeTrackingBackfillBatch(
      backfillSettings.backfillTableFullName,
      schema,
      targetTableSettings.targetTableFullName,
      tablePropertiesSettings,
      watermark
    )

/** The companion object for the MsSqlBackfillBatchFactory class.
  */
object MsSqlBackfillOverwriteBatchFactory:

  /** The environment required for the MsSqlBackfillBatchFactory.
    */
  private type Environment = StagingPropertyManager & BackfillSettings & SinkSettings & TablePropertiesSettings

  /** Creates a new MsSqlBackfillBatchFactory.
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
      stagingPropertyManager: StagingPropertyManager,
      backfillSettings: BackfillSettings,
      targetTableSettings: SinkSettings,
      tablePropertiesSettings: TablePropertiesSettings
  ): MsSqlBackfillOverwriteBatchFactory =
    new MsSqlBackfillOverwriteBatchFactory(
      stagingPropertyManager,
      backfillSettings,
      targetTableSettings,
      tablePropertiesSettings
    )

  /** The ZLayer for the MsSqlBackfillBatchFactory.
    */
  val layer: ZLayer[Environment, Nothing, BackfillOverwriteBatchFactory] =
    ZLayer {
      for
        stagingPropertyManager  <- ZIO.service[StagingPropertyManager]
        backfillSettings        <- ZIO.service[BackfillSettings]
        targetTableSettings     <- ZIO.service[SinkSettings]
        tablePropertiesSettings <- ZIO.service[TablePropertiesSettings]
      yield MsSqlBackfillOverwriteBatchFactory(
        stagingPropertyManager,
        backfillSettings,
        targetTableSettings,
        tablePropertiesSettings
      )
    }
