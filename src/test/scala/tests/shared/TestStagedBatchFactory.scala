package com.sneaksanddata.arcane.framework
package tests.shared

import models.batches.{MergeableBatch, StagedVersionedBatch}
import models.queries.MergeQuery
import models.schemas.ArcaneSchema
import services.streaming.batching.StagedBatchFactory

import zio.{Task, ZIO}

class TestMergeBatch(stagedTableName: String, tableName: String, batchSchema: ArcaneSchema) extends StagedVersionedBatch with MergeableBatch:
  override val name: String = stagedTableName
  override val schema: ArcaneSchema = batchSchema
  override val targetTableName: String = tableName
  override val completedWatermarkValue: Option[String] = None
  override val batchQuery: MergeQuery = MergeQuery("test", "test")

  override def reduceExpr: String = ""
  

class TestWatermarkBatch(tableName: String, watermark: String) extends StagedVersionedBatch with MergeableBatch:
  override val name: String = "watermark"
  override val schema: ArcaneSchema = ArcaneSchema.empty()
  override val targetTableName: String = tableName

  override val batchQuery: MergeQuery = MergeQuery("test", "test")
  override val completedWatermarkValue: Option[String] = Some(watermark)

  override def reduceExpr: String = ""

class TestStagedBatchFactory extends StagedBatchFactory:
  override type OutputBatch = TestMergeBatch
  override type WatermarkBatch = TestWatermarkBatch

  override def createWatermarkBatch(targetTableName: String, watermark: String): Task[TestWatermarkBatch] =
    ZIO.succeed(new TestWatermarkBatch(targetTableName, watermark))

  override def createDataBatch(stagedTableName: String, targetTableName: String, batchSchema: ArcaneSchema): Task[TestMergeBatch] =
    ZIO.succeed(new TestMergeBatch(stagedTableName, targetTableName, batchSchema))
