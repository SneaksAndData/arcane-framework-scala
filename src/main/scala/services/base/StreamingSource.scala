package com.sneaksanddata.arcane.framework
package services.base

import models.schemas.{ArcaneSchema, DataCell, DataRow, MergeKeyField, given_CanAdd_ArcaneSchema}
import models.settings.sources.{DataRowModification, SurrogateMergeKeyImpl}
import utils.HashUtils

import zio.{Task, ZIO}

trait StreamingSource extends SchemaProvider[ArcaneSchema] with ShardProvider

trait PrimaryKeyProvider:
  protected def primaryKeyNames: Task[Seq[String]]

abstract class DefaultStreamingSource(protected val modifications: Seq[DataRowModification])
    extends StreamingSource
    with PrimaryKeyProvider {
  protected def getSourceSchema: Task[ArcaneSchema]

  protected def applyDataRowModification(
      row: DataRow,
      modification: DataRowModification
  ): Task[DataRow] = modification match
    case SurrogateMergeKeyImpl(_) =>
      addSurrogateMergeKey(row)
    case _ => ZIO.succeed(row)

  protected def applySchemaModification(
      schema: ArcaneSchema,
      modification: DataRowModification
  ): Task[ArcaneSchema] = modification match
    case SurrogateMergeKeyImpl(_) if !schema.exists(_.name.equalsIgnoreCase(MergeKeyField.name)) =>

      // Currently for JSON source need to use non-indexed fieldsok,
      val newSchema =
        if schema.isIndexed then
          schema.addIndexedField(
            MergeKeyField.name,
            MergeKeyField.fieldType
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
        // Covers String and org.apache.avro.util.Utf8
        case value: CharSequence => value.toString
        case null                => throw new IllegalArgumentException("PK value must not be null")
        case other => throw new UnsupportedOperationException(s"Unsupported PK type: ${other.getClass.getName}")
      }
      .mkString("#")

    HashUtils.murmur3(input)
}
