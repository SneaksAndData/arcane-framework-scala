package com.sneaksanddata.arcane.framework
package services.synapse

import services.streaming.base.StreamDataProvider
import models.DataRow
import models.app.StreamContext
import models.settings.VersionedDataGraphBuilderSettings
import services.synapse.base.SynapseLinkDataProvider

import logging.ZIOLogAnnotations.*
import zio.ZIO
import zio.stream.ZStream

class SynapseLinkStreamingDataProvider(dataProvider: SynapseLinkDataProvider,
                                       settings: VersionedDataGraphBuilderSettings,
                                       streamContext: StreamContext) extends StreamDataProvider:
  override type StreamElementType = DataRow

  override def stream: ZStream[Any, Throwable, DataRow] = if streamContext.IsBackfilling then
      dataProvider.requestBackfill
  else
    ZStream.unfold(ZStream.succeed(Option.empty[String]))(version => Some(
      version.flatMap(dataProvider.requestChanges).map(r => r._1),
      version.flatMap(dataProvider.requestChanges).take(1).map(v => Some(v._2))
    )).flatten
