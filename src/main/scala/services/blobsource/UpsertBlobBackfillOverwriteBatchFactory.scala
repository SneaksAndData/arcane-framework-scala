package com.sneaksanddata.arcane.framework
package services.blobsource

import models.batches.{StagedBackfillOverwriteBatch, UpsertBlobBackfillOverwriteBatch}
import models.settings.TablePropertiesSettings
import models.settings.backfill.BackfillSettings
import models.settings.sink.SinkSettings
import services.iceberg.base.StagingPropertyManager
import services.streaming.base.BackfillOverwriteBatchFactory
import services.iceberg.given_Conversion_Schema_ArcaneSchema

import zio.{Task, ZIO, ZLayer}

/** A factory that creates a backfill batch for the Blob Source.
  * @param backfillSettings
  *   The backfill settings.
  * @param targetTableSettings
  *   The target table settings.
  * @param tablePropertiesSettings
  *   The table properties settings.
  */
class UpsertBlobBackfillOverwriteBatchFactory(
    stagingTablePropertyManager: StagingPropertyManager,
    backfillSettings: BackfillSettings,
    targetTableSettings: SinkSettings,
    tablePropertiesSettings: TablePropertiesSettings
) extends BackfillOverwriteBatchFactory:

  /** @inheritdoc
    */
  def createBackfillBatch(watermark: Option[String]): Task[StagedBackfillOverwriteBatch] =
    for schema <- stagingTablePropertyManager.getTableSchema(backfillSettings.backfillTableNameParts.Name)
    yield UpsertBlobBackfillOverwriteBatch(
      backfillSettings.backfillTableFullName,
      schema,
      targetTableSettings.targetTableFullName,
      tablePropertiesSettings,
      watermark
    )

/** The companion object for the BlobSourceBackfillOverwriteBatchFactory class.
  */
object UpsertBlobBackfillOverwriteBatchFactory:

  /** The environment required for the BlobSourceBackfillOverwriteBatchFactory.
    */
  private type Environment = StagingPropertyManager & BackfillSettings & SinkSettings & TablePropertiesSettings

  /** Creates a new BlobSourceBackfillOverwriteBatchFactory.
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
  ): UpsertBlobBackfillOverwriteBatchFactory =
    new UpsertBlobBackfillOverwriteBatchFactory(
      stagingPropertyManager,
      backfillSettings,
      targetTableSettings,
      tablePropertiesSettings
    )

  /** The ZLayer for the BlobSourceBackfillOverwriteBatchFactory.
    */
  val layer: ZLayer[Environment, Nothing, BackfillOverwriteBatchFactory] =
    ZLayer {
      for
        stagingPropertyManager  <- ZIO.service[StagingPropertyManager]
        backfillSettings        <- ZIO.service[BackfillSettings]
        targetTableSettings     <- ZIO.service[SinkSettings]
        tablePropertiesSettings <- ZIO.service[TablePropertiesSettings]
      yield UpsertBlobBackfillOverwriteBatchFactory(
        stagingPropertyManager,
        backfillSettings,
        targetTableSettings,
        tablePropertiesSettings
      )
    }
