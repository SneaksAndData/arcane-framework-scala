package com.sneaksanddata.arcane.framework
package services.streaming.graph_builders.base

import services.streaming.base.{RowGroupTransformer, StagedBatchProcessor}

import com.sneaksanddata.arcane.framework.models.DataRow
import zio.stream.ZPipeline
import zio.{Chunk, ZIO}

import java.util.UUID

trait IStagingProcessor extends RowGroupTransformer:
 
  override type OutgoingElement = StagedBatchProcessor#BatchType
  override type IncomingElement = DataRow | Any

  override def process(onStagingTablesComplete: OnStagingTablesComplete, onBatchStaged: OnBatchStaged): ZPipeline[Any, Throwable, Chunk[IncomingElement], OutgoingElement]
