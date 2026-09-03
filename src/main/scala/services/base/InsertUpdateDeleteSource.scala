package com.sneaksanddata.arcane.framework
package services.base

import models.schemas.*
import models.settings.sources.modification.*
import exceptions.FatalStreamFailException
import utils.HashUtils

import zio.{Task, UIO, ZIO}

trait PrimaryKeyProvider:
  protected def getPrimaryKey: Task[FrozenSurrogateMergeKey]

trait VersionProvider:
  protected def versionFieldName: Task[String]

abstract class InsertUpdateDeleteSource(suppliedModifications: Seq[DataRowModification])
    extends DefaultStreamingSource(suppliedModifications)
    with PrimaryKeyProvider
    with VersionProvider:

  override lazy val allModifications: Task[Seq[DataRowModification]] =
    getPrimaryKey.map(suppliedModifications ++ Seq(_))

  override protected def applyDataRowModification(
      row: DataRow,
      modification: DataRowModification
  ): DataRow = modification match
    case FrozenSurrogateMergeKey(fieldNames) => addSurrogateMergeKey(row, fieldNames)
    case SurrogateVersionImpl(_)             => row // addSurrogateVersion(row)
    case _                                   => row

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

  private def addSurrogateMergeKey(row: DataRow, keys: Set[String]): DataRow =
    val keyValues = row.filter(cell => keys.contains(cell.name.toLowerCase))

    if keyValues.size != keys.size then
      throw FatalStreamFailException(
        s"Some primary-key fields are missing or have NULL values. Required: ${keys.mkString(",")}, found: ${keyValues.map(_.name).mkString(",")}. Please review source configuration."
      )

    val valueToHash = keyValues.map(_.value.toString).mkString("#")
    val mergeKey    = HashUtils.murmur3(valueToHash)

    row :+ DataCell(
      name = MergeKeyField.name,
      Type = MergeKeyField.fieldType,
      value = mergeKey
    )
