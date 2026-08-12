package com.sneaksanddata.arcane.framework
package services.base

import models.schemas.*
import models.settings.sources.{DataRowModification, DataRowSchemaVersion, SurrogateMergeKeyImpl}

import zio.{Task, ZIO}

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.HexFormat

trait StreamingSource extends SchemaProvider[ArcaneSchema] with ShardProvider

trait PrimaryKeyProvider:
  protected def primaryKeyNames: Task[Seq[String]]

abstract class DefaultStreamingSource(
    protected val modifications: Seq[DataRowModification],
    protected val dataRowSchemaVersion: DataRowSchemaVersion
) extends StreamingSource
    with PrimaryKeyProvider {
  require(
    dataRowSchemaVersion != DataRowSchemaVersion.V1 ||
      modifications.exists {
        case SurrogateMergeKeyImpl(_) => true
        case _                        => false
      },
    "Data-row schema V1 requires the surrogateMergeKey modification"
  )

  protected def getSourceSchema: Task[ArcaneSchema]

  protected def applyDataRowModification(
      row: DataRow,
      modification: DataRowModification
  ): Task[DataRow] = modification match
    case SurrogateMergeKeyImpl(_) if dataRowSchemaVersion.usesCommonMergeKey =>
      addSurrogateMergeKey(row)
    case _ => ZIO.succeed(row)

  protected def applySchemaModification(
      schema: ArcaneSchema,
      modification: DataRowModification
  ): Task[ArcaneSchema] = modification match
    case SurrogateMergeKeyImpl(_)
        if dataRowSchemaVersion.usesCommonMergeKey &&
          !schema.exists(_.name.equalsIgnoreCase(MergeKeyField.name)) =>

      val newSchema =
        if schema.isIndexed then
          val nextFieldId = schema.collect { case field: IndexedArcaneSchemaField =>
            field.fieldId
          }.max + 1
          schema.addIndexedField(
            MergeKeyField.name,
            MergeKeyField.fieldType,
            nextFieldId
          )
        else
          schema.addField(
            MergeKeyField.name,
            MergeKeyField.fieldType
          )

      ZIO.succeed(newSchema)
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
    val input = keyValues
      .map {
        case s: String => s
        case null      => throw new IllegalArgumentException("PK value must not be null")
        case other     => throw new UnsupportedOperationException(s"Unsupported PK type: ${other.getClass.getName}")
      }
      .mkString("#")

    val digest = MessageDigest
      .getInstance("SHA-256")
      .digest(input.getBytes(StandardCharsets.UTF_8))

    HexFormat.of().formatHex(digest)
}
