package com.sneaksanddata.arcane.framework
package services.blobsource

import models.batches.{StagedBackfillOverwriteBatch, UpsertBlobBackfillOverwriteBatch}
import models.settings.TablePropertiesSettings
import models.settings.backfill.BackfillSettings
import models.settings.sink.SinkSettings
import services.iceberg.base.StagingPropertyManager
import services.streaming.base.BackfillOverwriteBatchFactory
import services.iceberg.given_Conversion_Schema_ArcaneSchema

import com.sneaksanddata.arcane.framework.models.app.PluginStreamContext
import zio.{Task, ZIO, ZLayer}

/** A factory that creates a backfill batch for the Blob Source.
  * @param backfillSettings
  *   The backfill settings.
  * @param targetTableSettings
  *   The target table settings.
  */
class UpsertBlobBackfillOverwriteBatchFactory(
    stagingTablePropertyManager: StagingPropertyManager,
    backfillSettings: BackfillSettings,
    targetTableSettings: SinkSettings
) extends BackfillOverwriteBatchFactory:

  /** @inheritdoc
    */
  def createBackfillBatch(watermark: Option[String]): Task[StagedBackfillOverwriteBatch] =
    for schema <- stagingTablePropertyManager.getTableSchema(backfillSettings.backfillTableNameParts.Name)
    yield UpsertBlobBackfillOverwriteBatch(
      backfillSettings.backfillTableFullName,
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
    * @param backfillSettings
    *   The backfill settings.
    * @param targetTableSettings
    *   The target table settings.
    * @return
    *   The SynapseBackfillOverwriteBatchFactory instance.
    */
  def apply(
      stagingPropertyManager: StagingPropertyManager,
      backfillSettings: BackfillSettings,
      targetTableSettings: SinkSettings
  ): UpsertBlobBackfillOverwriteBatchFactory =
    new UpsertBlobBackfillOverwriteBatchFactory(
      stagingPropertyManager,
      backfillSettings,
      targetTableSettings
    )

  /** The ZLayer for the BlobSourceBackfillOverwriteBatchFactory.
    */
  val layer: ZLayer[Environment, Nothing, BackfillOverwriteBatchFactory] =
    ZLayer {
      for
        stagingPropertyManager  <- ZIO.service[StagingPropertyManager]
        context <- ZIO.service[PluginStreamContext]
      yield UpsertBlobBackfillOverwriteBatchFactory(
        stagingPropertyManager,
        context.streamMode.backfill,
        context.sink
      )
    }
