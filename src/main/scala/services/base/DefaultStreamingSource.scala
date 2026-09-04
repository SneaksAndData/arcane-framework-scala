package com.sneaksanddata.arcane.framework
package services.base

import models.schemas.*
import models.schemas.given_CanAdd_ArcaneSchema
import models.settings.sources.modification.*
import extensions.ZExtensions.combineWith

import zio.stream.ZStream
import zio.{Chunk, Task, UIO, ZIO}

abstract class DefaultStreamingSource(protected val modifications: Seq[DataRowModification]) extends StreamingSource {

  final override lazy val getSchema: Task[ArcaneSchema] =
    getSourceSchema.combineWith(allModifications).flatMap { case (schema, mods) =>
      applySchemaModifications(schema, mods)
    }

  protected lazy val allModifications: Task[Seq[DataRowModification]] = ZIO.succeed(modifications)

  protected def getSourceSchema: Task[ArcaneSchema]

  protected def applyDataRowModification(
      rows: Chunk[DataRow],
      modification: DataRowModification
  ): Chunk[DataRow] = rows

  protected def applySchemaModification(
      schema: ArcaneSchema,
      modification: DataRowModification
  ): Task[ArcaneSchema] = ZIO.succeed(schema)

  final def applyDataRowModifications(rows: Chunk[DataRow], supplied: Seq[DataRowModification]): Chunk[DataRow] =
    supplied.foldLeft(rows)((agg, mod) => applyDataRowModification(agg, mod))

  final def applySchemaModifications(schema: ArcaneSchema, supplied: Seq[DataRowModification]): Task[ArcaneSchema] =
    ZIO.foldLeft(supplied)(schema)(applySchemaModification)

  protected def addFieldToSchema(field: ArcaneSchemaField, schema: ArcaneSchema): Task[ArcaneSchema] =
    val newSchema =
      if !schema.exists(_.name.equalsIgnoreCase(field.name)) then
        // Currently for JSON source need to use non-indexed fields
        if schema.isIndexed then schema.addIndexedField(field.name, field.fieldType)
        else schema.addField(field.name, field.fieldType)
      else schema
    ZIO.succeed(newSchema)
}
