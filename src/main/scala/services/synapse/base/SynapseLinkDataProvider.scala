package com.sneaksanddata.arcane.framework
package services.synapse.base

import models.schemas.DataRow
import models.settings.VersionedDataGraphBuilderSettings
import models.settings.backfill.BackfillSettings
import models.settings.sink.SinkSettings
import services.iceberg.base.SinkPropertyManager
import services.streaming.base.DefaultSourceDataProvider
import services.streaming.throughput.base.ThroughputShaperBuilder
import services.synapse.versioning.SynapseWatermark
import services.synapse.versioning.SynapseWatermark.*

import zio.stream.ZStream
import zio.{Task, ZIO, ZLayer}

import java.time.OffsetDateTime

class SynapseLinkDataProvider(
    synapseReader: SynapseLinkReader,
    sinkPropertyManager: SinkPropertyManager,
    sinkSettings: SinkSettings,
    settings: VersionedDataGraphBuilderSettings,
    backfillSettings: BackfillSettings,
    throughputShaperBuilder: ThroughputShaperBuilder
) extends DefaultSourceDataProvider[SynapseWatermark](
      sinkPropertyManager,
      sinkSettings,
      backfillSettings,
      throughputShaperBuilder
    ):

  override protected def backfillStream(backfillStartDate: Option[OffsetDateTime]): ZStream[Any, Throwable, DataRow] =
    backfillStartDate match
      case Some(backfillStartDate) => synapseReader.getData(backfillStartDate)
      case None                    => ZStream.fail(new IllegalArgumentException("Backfill start date is not set"))

  override def hasChanges(previousVersion: SynapseWatermark): Task[Boolean] =
    synapseReader.hasChanges(previousVersion)

  override def getCurrentVersion(previousVersion: SynapseWatermark): Task[SynapseWatermark] =
    synapseReader.getCurrentVersion(previousVersion)

  /** Implements data streaming logic for public `requestChanges`
    *
    * @param previousVersion
    *   Previous watermark
    * @return
    */
  override protected def changeStream(previousVersion: SynapseWatermark): ZStream[Any, Throwable, DataRow] =
    synapseReader.getChanges(previousVersion)

  /** Evaluates watermark to be used when evaluating current snapshot version at the start of a backfill process
    * @return
    */
  override protected def getBackfillStartWatermark(startTime: Option[OffsetDateTime]): SynapseWatermark =
    SynapseWatermark.epoch

object SynapseLinkDataProvider:
  type Environment = VersionedDataGraphBuilderSettings & BackfillSettings & SynapseLinkReader & SinkPropertyManager &
    SinkSettings & ThroughputShaperBuilder

  val layer: ZLayer[Environment, Throwable, SynapseLinkDataProvider] = ZLayer {
    for
      versionedSettings <- ZIO.service[VersionedDataGraphBuilderSettings]
      propertyManager   <- ZIO.service[SinkPropertyManager]
      sinkSettings      <- ZIO.service[SinkSettings]
      backfillSettings  <- ZIO.service[BackfillSettings]
      synapseReader     <- ZIO.service[SynapseLinkReader]
      shaperBuilder     <- ZIO.service[ThroughputShaperBuilder]
    yield SynapseLinkDataProvider(
      synapseReader,
      propertyManager,
      sinkSettings,
      versionedSettings,
      backfillSettings,
      shaperBuilder
    )
  }
