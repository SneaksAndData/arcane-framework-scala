package com.sneaksanddata.arcane.framework
package services.pullstream

import models.batches.{PullStreamChangeTrackingMergeBatch, PullStreamChangeTrackingWatermarkOnlyBatch}
import models.schemas.ArcaneSchema
import models.settings.EmptyTablePropertiesSettings
import services.streaming.batching.StagedBatchFactory

import zio.{Task, ZIO, ZLayer}

/** Raised when the configured version column is absent from the batch schema. Without it the generated MERGE would
  * reference an unknown column and fail inside the query engine with a message that gives no hint about the cause.
  */
final class MissingVersionFieldException(message: String) extends RuntimeException(message)

/** @param versionFieldName
  *   Column used to order concurrent versions of the same merge key. It must be a column of the sink table, and is
  *   taken from the source's `watermarkFieldName` so that it always matches the value the source projects into rows.
  */
class PullStreamStagedBatchFactory(val versionFieldName: String) extends StagedBatchFactory:
  override type OutputBatch    = PullStreamChangeTrackingMergeBatch
  override type WatermarkBatch = PullStreamChangeTrackingWatermarkOnlyBatch

  /** Resolves the version column against the batch schema, returning the schema's own spelling.
    *
    * The match is case-insensitive because engines that fold unquoted identifiers (Trino, for instance) cannot
    * distinguish `timestampUTC` from `TimestampUTC` anyway, so accepting either spelling here avoids a mismatch that
    * the engine would not have honoured in the first place.
    */
  private def resolveVersionField(batchSchema: ArcaneSchema): Task[String] =
    ZIO
      .fromOption(batchSchema.map(_.name).find(_.equalsIgnoreCase(versionFieldName)))
      .orElseFail(
        MissingVersionFieldException(
          s"Column '$versionFieldName' is not present in the sink table. The pull stream orders concurrent " +
            s"versions of a merge key by this column, so it must exist in the target table. Either add it to the " +
            s"target table, or point 'watermarkFieldName' at an existing column. Available columns: " +
            batchSchema.map(_.name).mkString(", ")
        )
      )

  override def createDataBatch(
      stagedTableName: String,
      targetTableName: String,
      batchSchema: ArcaneSchema
  ): Task[PullStreamChangeTrackingMergeBatch] =
    for versionField <- resolveVersionField(batchSchema)
    yield PullStreamChangeTrackingMergeBatch(
      stagedTableName,
      batchSchema,
      targetTableName,
      EmptyTablePropertiesSettings,
      versionField
    )

  override def createWatermarkBatch(
      targetTableName: String,
      watermark: String
  ): Task[PullStreamChangeTrackingWatermarkOnlyBatch] =
    ZIO.succeed(PullStreamChangeTrackingWatermarkOnlyBatch(targetTableName, watermark))

object PullStreamStagedBatchFactory:
  val layer: ZLayer[PullStreamingSource, Nothing, PullStreamStagedBatchFactory] =
    ZLayer {
      for source <- ZIO.service[PullStreamingSource]
      yield new PullStreamStagedBatchFactory(source.versionFieldName)
    }
