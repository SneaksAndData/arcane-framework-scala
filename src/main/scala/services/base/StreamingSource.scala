package com.sneaksanddata.arcane.framework
package services.base

import models.schemas.{ArcaneSchema, DataRow, MergeKeyField, given_CanAdd_ArcaneSchema}
import models.settings.sources.DataRowModification

import zio.{Task, ZIO}

trait StreamingSource extends SchemaProvider[ArcaneSchema] with ShardProvider

abstract class DefaultStreamingSource(
    protected val modifications: Seq[DataRowModification]
) extends StreamingSource {
  protected val getSourceSchema: Task[ArcaneSchema]

  protected def applyDataRowModification(
      row: DataRow,
      modification: DataRowModification
  ): Task[DataRow]

  protected def applySchemaModification(
      schema: ArcaneSchema,
      modification: DataRowModification
  ): Task[ArcaneSchema]

  final def applyDataRowModifications(row: DataRow): Task[DataRow] =
    ZIO.foldLeft(modifications)(row)(applyDataRowModification)

  final def applySchemaModifications(schema: ArcaneSchema): Task[ArcaneSchema] =
    ZIO.foldLeft(modifications)(schema)(applySchemaModification)

//  final override lazy val getSchema: Task[ArcaneSchema] = for
//    source <- getSourceSchema
//    ext    <- getSchemaExtensions(Seq.empty)
//  yield source.appendedAll(ext)

  final override lazy val getSchema: Task[ArcaneSchema] =
    getSourceSchema.flatMap(applySchemaModifications)
}
