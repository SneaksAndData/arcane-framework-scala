package com.sneaksanddata.arcane.framework
package services.base

import models.schemas.*
import models.schemas.given_CanAdd_ArcaneSchema
import models.settings.sources.modification.*

import zio.stream.ZStream
import zio.{Chunk, Task, UIO, ZIO}

abstract class DefaultStreamingSource(protected val modifications: Seq[DataRowModification]) extends StreamingSource {

  final override lazy val getSchema: Task[ArcaneSchema] =
    getSourceSchema.flatMap(applySchemaModifications)

  protected lazy val allModifications: Task[Seq[DataRowModification]] = ZIO.succeed(modifications)

  protected def getSourceSchema: Task[ArcaneSchema]

  protected def applyDataRowModification(
      row: DataRow,
      modification: DataRowModification
  ): DataRow = row

  protected def applySchemaModification(
      schema: ArcaneSchema,
      modification: DataRowModification
  ): Task[ArcaneSchema] = ZIO.succeed(schema)

  final def applyDataRowModifications(rows: Chunk[DataRow], supplied: Seq[DataRowModification]): Chunk[DataRow] =
    supplied.foldLeft(rows)((agg, e) => agg.map(applyDataRowModification(_, e)))

  final def applySchemaModifications(schema: ArcaneSchema): Task[ArcaneSchema] =
    ZIO.foldLeft(modifications)(schema)(applySchemaModification)

  protected def addFieldToSchema(field: ArcaneSchemaField, schema: ArcaneSchema): Task[ArcaneSchema] =
    val newSchema =
      if !schema.exists(_.name.equalsIgnoreCase(field.name)) then
        // Currently for JSON source need to use non-indexed fields
        if schema.isIndexed then schema.addIndexedField(field.name, field.fieldType)
        else schema.addField(field.name, field.fieldType)
      else schema
    ZIO.succeed(newSchema)
}
