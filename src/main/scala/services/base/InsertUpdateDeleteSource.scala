package com.sneaksanddata.arcane.framework
package services.base

import models.schemas.*
import models.settings.sources.modification.*
import exceptions.FatalStreamFailException
import utils.HashUtils
import InsertUpdateDeleteSource.*

import zio.{Task, ZIO}

trait PrimaryKeyProvider:
  protected def primaryKeyNames: Task[Seq[String]]

trait VersionProvider:
  protected def versionFieldName: Task[String]

abstract class InsertUpdateDeleteSource(suppliedModifications: Seq[DataRowModification])
    extends DefaultStreamingSource(addRequiredModifications(suppliedModifications))
    with PrimaryKeyProvider
    with VersionProvider {

  override protected def applyDataRowModification(
      row: DataRow,
      modification: DataRowModification
  ): Task[DataRow] = modification match
    case SurrogateMergeKeyImpl(_) => addSurrogateMergeKey(row)
    case SurrogateVersionImpl(_)  => addSurrogateVersion(row)
    case _                        => ZIO.succeed(row)

  override protected def applySchemaModification(
      schema: ArcaneSchema,
      modification: DataRowModification
  ): Task[ArcaneSchema] = modification match
    case SurrogateMergeKeyImpl(_) => addFieldToSchema(MergeKeyField, schema)
    case SurrogateVersionImpl(_)  => addFieldToSchema(VersionField, schema)
    case _                        => ZIO.succeed(schema)

  private def addSurrogateVersion(row: DataRow): Task[DataRow] =
    for
      sourceVersionName <- versionFieldName
      versionCell <- ZIO
        .fromOption(row.find(_.name.equalsIgnoreCase(sourceVersionName)))
        .orElseFail(FatalStreamFailException(s"Version field '$sourceVersionName' is missing from the source row"))
      versionValue <- versionCell.value match
        case value: Long =>
          ZIO.succeed(value)
        case null =>
          ZIO.succeed(null)
        case value =>
          ZIO.fail(
            FatalStreamFailException(
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
}

object InsertUpdateDeleteSource {
  private def addRequiredModifications(originalModifications: Seq[DataRowModification]): Seq[DataRowModification] =
    originalModifications.filter {
      case SurrogateMergeKeyImpl(_) => false
      case SurrogateVersionImpl(_)  => false
      case _                        => true
    } ++ Seq(
      SurrogateMergeKeyImpl(SurrogateMergeKey()),
      SurrogateVersionImpl(SurrogateVersion())
    )

  private def getPrimaryKeyValue(row: DataRow, key: String): Task[Any] =
    ZIO
      .fromOption(row.find(_.name.equalsIgnoreCase(key)))
      .orElseFail(FatalStreamFailException(s"Primary-key field '$key' is missing from the source row"))
      .flatMap(cell =>
        ZIO
          .fromOption(Option(cell.value))
          .orElseFail(FatalStreamFailException(s"Primary-key field '$key' is null"))
      )

  private def createMergeKey(keyValues: Seq[Any]): String =
    val input = keyValues
      .map {
        // Covers String and org.apache.avro.util.Utf8
        case value: CharSequence => value.toString
        case null                => throw FatalStreamFailException("PK value must not be null")
        case other               => throw FatalStreamFailException(s"Unsupported PK type: ${other.getClass.getName}")
      }
      .mkString("#")

    HashUtils.murmur3(input)
}
