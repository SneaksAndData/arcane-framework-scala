package com.sneaksanddata.arcane.framework
package services.base

import models.schemas.{
  ArcaneSchema,
  ArcaneSchemaField,
  DataCell,
  DataRow,
  MergeKeyField,
  VersionField,
  given_CanAdd_ArcaneSchema
}
import models.settings.sources.{DataRowModification, SurrogateMergeKeyImpl, SurrogateVersionImpl}
import utils.HashUtils

import zio.{Task, ZIO}

trait StreamingSource extends SchemaProvider[ArcaneSchema] with ShardProvider

trait PrimaryKeyProvider:
  protected def primaryKeyNames: Task[Seq[String]]

trait VersionProvider:
  protected def versionName: Task[String]

abstract class DefaultStreamingSource(protected val modifications: Seq[DataRowModification])
    extends StreamingSource
    with PrimaryKeyProvider
    with VersionProvider {
  protected def getSourceSchema: Task[ArcaneSchema]

  protected def applyDataRowModification(
      row: DataRow,
      modification: DataRowModification
  ): Task[DataRow] = modification match
    case SurrogateMergeKeyImpl(_) => addSurrogateMergeKey(row)
    case SurrogateVersionImpl(_)  => addSurrogateVersion(row)
    case _                        => ZIO.succeed(row)

  protected def applySchemaModification(
      schema: ArcaneSchema,
      modification: DataRowModification
  ): Task[ArcaneSchema] = modification match
    case SurrogateMergeKeyImpl(_) => addFieldToSchema(MergeKeyField, schema)
    case SurrogateVersionImpl(_)  => addFieldToSchema(VersionField, schema)
    case _                        => ZIO.succeed(schema)

  final def applyDataRowModifications(row: DataRow): Task[DataRow] =
    ZIO.foldLeft(modifications)(row)(applyDataRowModification)

  final def applySchemaModifications(schema: ArcaneSchema): Task[ArcaneSchema] =
    ZIO.foldLeft(modifications)(schema)(applySchemaModification)

  final override lazy val getSchema: Task[ArcaneSchema] =
    getSourceSchema.flatMap(applySchemaModifications)

  private def addFieldToSchema(field: ArcaneSchemaField, schema: ArcaneSchema): Task[ArcaneSchema] =
    val newSchema =
      if !schema.exists(_.name.equalsIgnoreCase(field.name)) then
        // Currently for JSON source need to use non-indexed fields
        if schema.isIndexed then schema.addIndexedField(field.name, field.fieldType)
        else schema.addField(field.name, field.fieldType)
      else schema
    ZIO.succeed(newSchema)

  private def addSurrogateVersion(row: DataRow): Task[DataRow] =
    for
      sourceVersionName <- versionName
      versionCell <- ZIO
        .fromOption(row.find(_.name.equalsIgnoreCase(sourceVersionName)))
        .orElseFail(new IllegalArgumentException(s"Version field '$sourceVersionName' is missing from the source row"))
      versionValue <- versionCell.value match
        case value: Long =>
          ZIO.succeed(value)
        case null =>
          ZIO.fail(new IllegalArgumentException(s"Version field '$sourceVersionName' must not be null"))
        case value =>
          ZIO.fail(
            new IllegalArgumentException(
              s"Version field '$sourceVersionName' must contain a Long value, but contains ${value.getClass.getName}"
            )
          )
    yield row.filterNot(_.name.equalsIgnoreCase(VersionField.name)) :+ DataCell(
      name = VersionField.name,
      Type = VersionField.fieldType,
      value = versionValue
    )

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
