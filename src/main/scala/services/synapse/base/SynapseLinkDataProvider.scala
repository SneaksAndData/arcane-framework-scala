package com.sneaksanddata.arcane.framework
package services.synapse.base

import logging.ZIOLogAnnotations.zlog
import models.schemas.JsonWatermarkRow
import models.settings.{BackfillSettings, SinkSettings, VersionedDataGraphBuilderSettings}
import services.iceberg.base.TablePropertyManager
import services.streaming.base.{BackfillDataProvider, VersionedDataProvider}
import services.synapse.SynapseLinkBatch
import services.synapse.versioning.SynapseWatermark

import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

import java.time.OffsetDateTime
import scala.util.Try

class SynapseLinkDataProvider(
                               synapseReader: SynapseLinkReader,
                               propertyManager: TablePropertyManager,
                               sinkSettings: SinkSettings,
                               settings: VersionedDataGraphBuilderSettings,
                               backfillSettings: BackfillSettings
) extends VersionedDataProvider[SynapseWatermark, SynapseLinkBatch]
    with BackfillDataProvider[SynapseLinkBatch]:

  override def requestChanges(
      previousVersion: SynapseWatermark,
      nextVersion: SynapseWatermark
  ): ZStream[Any, Throwable, SynapseLinkBatch] =
    synapseReader.getChanges(previousVersion).concat(ZStream.succeed(JsonWatermarkRow(nextVersion)))

  override def requestBackfill: ZStream[Any, Throwable, SynapseLinkBatch] = backfillSettings.backfillStartDate match
    case Some(backfillStartDate) =>
      ZStream
        .fromZIO(getCurrentVersion(SynapseWatermark.epoch))
        .flatMap(version => synapseReader.getData(backfillStartDate).concat(ZStream.succeed(JsonWatermarkRow(version))))
    case None => ZStream.fail(new IllegalArgumentException("Backfill start date is not set"))

  override def firstVersion: Task[SynapseWatermark] =
    for
      watermarkString <- propertyManager.getProperty(sinkSettings.targetTableNameParts.Name, "comment")
      _ <- zlog("Current watermark value on %s is '%s'", sinkSettings.targetTableFullName, watermarkString)
      watermark <- ZIO.attempt(Try(SynapseWatermark.fromJson(watermarkString)).toOption)
      fallback <- ZIO.when(watermark.isEmpty) {
        for
          startTime <- ZIO.succeed(OffsetDateTime.now())
          _ <- zlog(
            "Fetching version for the first iteration using legacy lookbackInterval, from %s",
            startTime.minus(settings.lookBackInterval).toString
          )
          result <- synapseReader.getVersion(startTime.minus(settings.lookBackInterval))
          _      <- zlog("Retrieved version %s", result.version)
        yield result
      }
    // assume fallback is only there if we have no watermark
    yield fallback match {
      // if fallback is computed, return it
      case Some(value) => value
      // if no fallback, get value from watermark and fail if it is empty
      case None => watermark.get
    }

  override def hasChanges(previousVersion: SynapseWatermark): Task[Boolean] =
    synapseReader.hasChanges(previousVersion)

  override def getCurrentVersion(previousVersion: SynapseWatermark): Task[SynapseWatermark] =
    synapseReader.getCurrentVersion(previousVersion)

object SynapseLinkDataProvider:
  type Environment = VersionedDataGraphBuilderSettings & BackfillSettings & SynapseLinkReader & TablePropertyManager &
    SinkSettings

  val layer: ZLayer[Environment, Throwable, SynapseLinkDataProvider] = ZLayer {
    for
      versionedSettings      <- ZIO.service[VersionedDataGraphBuilderSettings]
      propertyManager <- ZIO.service[TablePropertyManager]
      sinkSettings    <- ZIO.service[SinkSettings]
      backfillSettings       <- ZIO.service[BackfillSettings]
      synapseReader          <- ZIO.service[SynapseLinkReader]
    yield SynapseLinkDataProvider(
      synapseReader,
      propertyManager,
      sinkSettings,
      versionedSettings,
      backfillSettings
    )
  }
