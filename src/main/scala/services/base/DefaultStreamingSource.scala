package com.sneaksanddata.arcane.framework
package services.base

import models.schemas.*
import models.schemas.given_CanAdd_ArcaneSchema
import models.settings.sources.modification.*
import services.time.TimestampProvider
import extensions.ZExtensions.combineWith

import zio.{Chunk, Task, ZIO}

abstract class DefaultStreamingSource(
    protected val modifications: Seq[DataRowModification],
    protected val timestampProvider: TimestampProvider
) extends StreamingSource {

  final override lazy val getSchema: Task[ArcaneSchema] =
    getSourceSchema.combineWith(allModifications).flatMap { case (schema, mods) =>
      applySchemaModifications(schema, mods)
    }

  protected lazy val allModifications: Task[Seq[DataRowModification]] = ZIO.succeed(modifications)

  protected def getSourceSchema: Task[ArcaneSchema]

  protected def applyDataRowModification(
      rows: Chunk[DataRow],
      modification: DataRowModification
  ): Chunk[DataRow] = modification match {
    case LoadTimestampImpl(_) => addLoadTimestamp(rows)
    case _                    => rows
  }

  protected def applySchemaModification(
      schema: ArcaneSchema,
      modification: DataRowModification
  ): Task[ArcaneSchema] = modification match {
    case LoadTimestampImpl(_) => addFieldToSchema(LoadTimestampField, schema)
    case _                    => ZIO.succeed(schema)
  }

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

  private def addLoadTimestamp(rows: Chunk[DataRow]): Chunk[DataRow] =
    val timestamp = timestampProvider.timestamp

    rows.map { row =>
      row.filterNot(_.name.equalsIgnoreCase(LoadTimestampField.name)) :+ DataCell(
        name = LoadTimestampField.name,
        Type = LoadTimestampField.fieldType,
        value = timestamp
      )
    }
}
