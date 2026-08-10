package com.sneaksanddata.arcane.framework
package services.base

import models.schemas.{ArcaneSchema, DataCell, DataRow, MergeKeyField, given_CanAdd_ArcaneSchema}
import models.settings.sources.{DataRowModification, SurrogateMergeKeyImpl}

import zio.{Task, ZIO}

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.HexFormat

trait StreamingSource extends SchemaProvider[ArcaneSchema] with ShardProvider

trait PrimaryKeyProvider:
  protected def primaryKeyNames: Task[Seq[String]]

abstract class DefaultStreamingSource(
    protected val modifications: Seq[DataRowModification]
) extends StreamingSource
    with PrimaryKeyProvider {
  protected def getSourceSchema: Task[ArcaneSchema]

  protected def applyDataRowModification(
      row: DataRow,
      modification: DataRowModification
  ): Task[DataRow] = modification match
    case SurrogateMergeKeyImpl(_) => addSurrogateMergeKey(row)
    case _                        => ZIO.succeed(row)

  protected def applySchemaModification(
      schema: ArcaneSchema,
      modification: DataRowModification
  ): Task[ArcaneSchema] = modification match
    case SurrogateMergeKeyImpl(_) if !schema.exists(_.name.equalsIgnoreCase(MergeKeyField.name)) =>
      ZIO.succeed(schema.addField(MergeKeyField.name, MergeKeyField.fieldType))
    case _ => ZIO.succeed(schema)

  final def applyDataRowModifications(row: DataRow): Task[DataRow] =
    ZIO.foldLeft(modifications)(row)(applyDataRowModification)

  final def applySchemaModifications(schema: ArcaneSchema): Task[ArcaneSchema] =
    ZIO.foldLeft(modifications)(schema)(applySchemaModification)

  final override lazy val getSchema: Task[ArcaneSchema] =
    getSourceSchema.flatMap(applySchemaModifications)

  private def addSurrogateMergeKey(row: DataRow): Task[DataRow] =
    for
      keys      <- primaryKeyNames
      keyValues <- ZIO.foreach(keys)(getPrimaryKeyValue(row, _))
      mergeKey  <- ZIO.attempt(createMergeKey(keyValues))
    yield row.filterNot(_.name.equalsIgnoreCase(MergeKeyField.name)) :+ DataCell(
      name = MergeKeyField.name,
      Type = MergeKeyField.fieldType,
      value = mergeKey
    )

  private def getPrimaryKeyValue(row: DataRow, key: String): Task[Any] =
    ZIO
      .fromOption(row.find(_.name.equalsIgnoreCase(key)))
      .orElseFail(new IllegalArgumentException(s"Primary-key field '$key' is missing from the source row"))
      .flatMap(cell =>
        ZIO
          .fromOption(Option(cell.value))
          .orElseFail(new IllegalArgumentException(s"Primary-key field '$key' is null"))
      )

  private def createMergeKey(keyValues: Seq[Any]): String =
    val input = keyValues.map(_.toString.take(128)).mkString("#")
    val digest = MessageDigest
      .getInstance("SHA-256")
      .digest(input.getBytes(StandardCharsets.UTF_16LE))

    HexFormat.of().formatHex(digest)
}
