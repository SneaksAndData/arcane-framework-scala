package com.sneaksanddata.arcane.framework
package services.blobsource

import models.app.PluginStreamContext
import models.batches.{StagedBackfillOverwriteBatch, UpsertBlobBackfillOverwriteBatch}
import models.settings.TableName
import models.settings.TableNaming.*
import models.settings.backfill.BackfillSettings
import models.settings.sink.SinkSettings
import models.settings.staging.{StagingSettings, StagingTableSettings}
import services.iceberg.base.StagingPropertyManager
import services.iceberg.given_Conversion_Schema_ArcaneSchema
import services.streaming.base.BackfillOverwriteBatchFactory

import zio.{Task, ZIO, ZLayer}

/** A factory that creates a backfill batch for the Blob Source.
  */
class UpsertBlobBackfillOverwriteBatchFactory(
    stagingTablePropertyManager: StagingPropertyManager,
    stagingTableSettings: StagingTableSettings,
    targetTableSettings: SinkSettings
) extends BackfillOverwriteBatchFactory:

  /** @inheritdoc
    */
  def createBackfillBatch(watermark: Option[String]): Task[StagedBackfillOverwriteBatch] = for schema <-
      stagingTablePropertyManager.getTableSchema(stagingTableSettings.backfillTableName.parts.name)
  yield UpsertBlobBackfillOverwriteBatch(
    stagingTableSettings.backfillTableName,
    schema,
    targetTableSettings.targetTableFullName,
    targetTableSettings.targetTableProperties,
    watermark
  )

/** The companion object for the BlobSourceBackfillOverwriteBatchFactory class.
  */
object UpsertBlobBackfillOverwriteBatchFactory:

  /** The environment required for the BlobSourceBackfillOverwriteBatchFactory.
    */
  private type Environment = StagingPropertyManager & PluginStreamContext

  /** Creates a new BlobSourceBackfillOverwriteBatchFactory.
    *
    * @param targetTableSettings
    *   The target table settings.
    * @return
    *   The SynapseBackfillOverwriteBatchFactory instance.
    */
  def apply(
      stagingPropertyManager: StagingPropertyManager,
      stagingTableSettings: StagingTableSettings,
      targetTableSettings: SinkSettings
  ): UpsertBlobBackfillOverwriteBatchFactory =
    new UpsertBlobBackfillOverwriteBatchFactory(
      stagingPropertyManager,
      stagingTableSettings,
      targetTableSettings
    )

  /** The ZLayer for the BlobSourceBackfillOverwriteBatchFactory.
    */
  val layer: ZLayer[Environment, Nothing, BackfillOverwriteBatchFactory] =
    ZLayer {
      for
        stagingPropertyManager <- ZIO.service[StagingPropertyManager]
        context                <- ZIO.service[PluginStreamContext]
      yield UpsertBlobBackfillOverwriteBatchFactory(
        stagingPropertyManager,
        context.staging.table,
        context.sink
      )
    }
