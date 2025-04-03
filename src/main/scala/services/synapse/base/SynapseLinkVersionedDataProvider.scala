package com.sneaksanddata.arcane.framework
package services.synapse.base

import services.streaming.base.{BackfillDataProvider, VersionedDataProvider}
import services.synapse.SynapseLinkVersionedBatch
import models.DataRow
import models.settings.VersionedDataGraphBuilderSettings
import services.mssql.MsSqlConnection.BackfillBatch

import zio.{Task, ZIO}

import java.time.format.DateTimeFormatter
import java.time.{Duration, OffsetDateTime, ZoneOffset}


class SynapseLinkVersionedDataProvider(settings: VersionedDataGraphBuilderSettings) extends VersionedDataProvider[String, SynapseLinkVersionedBatch] with BackfillDataProvider:
  
  override def requestChanges(previousVersion: Option[String], lookBackInterval: Duration): Task[(Seq[DataRow], String)] = ???

  override def requestBackfill: Task[BackfillBatch] = ???

  override def firstVersion: Task[String] = ZIO.succeed(
    OffsetDateTime.now()
      .minus(settings.lookBackInterval)
      .format(
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH.mm.ss'Z'")
          .withZone(ZoneOffset.UTC)
      )
  )
