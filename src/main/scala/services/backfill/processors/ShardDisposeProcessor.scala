package com.sneaksanddata.arcane.framework
package services.backfill.processors

import services.streaming.base.StreamingBatchProcessor
import com.sneaksanddata.arcane.framework.models.batches.sharding.ShardCompletionBatch
import zio.stream.ZPipeline

class ShardDisposeProcessor extends StreamingBatchProcessor:
  override type BatchType = ShardCompletionBatch
  override def process: ZPipeline[Any, Throwable, BatchType, BatchType] = ???
