package com.sneaksanddata.arcane.framework
package services.blobsource.providers

import services.blobsource.{BlobSourceBatch, BlobSourceVersionedBatch}
import services.streaming.base.{BackfillDataProvider, VersionedDataProvider}

import com.sneaksanddata.arcane.framework.models.schemas.DataRow
import zio.Task
import zio.stream.ZStream

class BlobSourceDataProvider
    extends VersionedDataProvider[String, BlobSourceVersionedBatch]
    with BackfillDataProvider[BlobSourceBatch]:

  override def requestBackfill: ZStream[Any, Throwable, BlobSourceBatch] = ???

  override def requestChanges(previousVersion: String): ZStream[Any, Throwable, (DataRow, Long)] = ???

  override def firstVersion: Task[String] = ???
