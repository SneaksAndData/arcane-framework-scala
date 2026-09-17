package com.sneaksanddata.arcane.framework
package services.base

import models.schemas.*
import models.schemas.given_CanAdd_ArcaneSchema
import models.settings.sources.modification.*
import extensions.ZExtensions.combineWith

import zio.{Chunk, Task, ZIO}

import java.time.{Instant, OffsetDateTime, ZoneOffset}

abstract class DefaultStreamingSource(
    protected val modifications: Seq[DataRowModification]
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
    case SurrogateTimestampImpl(_)       => addLoadTimestamp(rows, None)
    case FrozenSurrogateTimestamp(value) => addLoadTimestamp(rows, Some(value))
    case _                               => rows
  }

  protected def applySchemaModification(
      schema: ArcaneSchema,
      modification: DataRowModification
  ): Task[ArcaneSchema] = modification match {
    case SurrogateTimestampImpl(_)   => addFieldToSchema(LoadTimestampField, schema)
    case FrozenSurrogateTimestamp(_) => addFieldToSchema(LoadTimestampField, schema)
    case _                           => ZIO.succeed(schema)
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

  private def addLoadTimestamp(rows: Chunk[DataRow], timestamp: Option[OffsetDateTime]): Chunk[DataRow] =
    rows.map { row =>
      row :+ DataCell(
        name = LoadTimestampField.name,
        Type = LoadTimestampField.fieldType,
        value = timestamp match
          case None     => OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC)
          case Some(ts) => ts
      )
    }
}
