package com.sneaksanddata.arcane.framework
package services.base

import exceptions.FatalStreamFailException
import extensions.ZExtensions.combineWith
import models.schemas.*
import models.settings.sources.modification.*
import utils.HashUtils

import zio.{Task, ZIO}

trait PrimaryKeyProvider:
  protected def getPrimaryKey: Task[FrozenSurrogateMergeKey]

trait VersionProvider:
  protected def getVersionField: Task[FrozenSurrogateVersion]

/** A streaming source that supports INSERT, UPDATE and DELETE data modifications. This source requires primary key
  * fields and a version field to be defined in concrete implementations.
  */
abstract class InsertUpdateDeleteSource(suppliedModifications: Seq[DataRowModification])
    extends DefaultStreamingSource(suppliedModifications)
    with PrimaryKeyProvider
    with VersionProvider:

  override lazy val allModifications: Task[Seq[DataRowModification]] =
    getPrimaryKey.combineWith(getVersionField).map { case (pk, v) =>
      suppliedModifications ++ Seq(pk, v)
    }

  override protected def applyDataRowModification(
      row: DataRow,
      modification: DataRowModification
  ): DataRow = modification match
    case FrozenSurrogateMergeKey(fieldNames) => addSurrogateMergeKey(row, fieldNames)
    case FrozenSurrogateVersion(fieldName)   => addSurrogateVersion(row, fieldName)
    case _                                   => row

  override protected def applySchemaModification(
      schema: ArcaneSchema,
      modification: DataRowModification
  ): Task[ArcaneSchema] = modification match
    case FrozenSurrogateMergeKey(_) => addFieldToSchema(MergeKeyField, schema)
    case FrozenSurrogateVersion(_)  => addFieldToSchema(VersionField, schema)
    case _                          => ZIO.succeed(schema)

  private def addSurrogateVersion(row: DataRow, sourceVersionFieldName: String): DataRow =
    val versionValue = row.find(_.name.equalsIgnoreCase(sourceVersionFieldName)) match
      case None =>
        throw FatalStreamFailException(s"Version field '$sourceVersionFieldName' is missing from the source row")
      case Some(cell) =>
        cell.value match
          case value: Long => value
          case null        => null
          case other =>
            throw FatalStreamFailException(
              s"Version field '$sourceVersionFieldName' must contain a Long value, but contains ${other.getClass.getName}"
            )

    row.filterNot(_.name.equalsIgnoreCase(VersionField.name)) :+ DataCell(
      name = VersionField.name,
      Type = VersionField.fieldType,
      value = versionValue
    )

  private def addSurrogateMergeKey(row: DataRow, keys: Seq[String]): DataRow =
    val keyValues = keys.flatMap(key => row.find(_.name.equalsIgnoreCase(key)))

    if keyValues.size != keys.size || keyValues.exists(_.value == null) then
      throw FatalStreamFailException(
        s"Some primary-key fields are missing or have NULL values. " +
          s"Required: ${keys.mkString(",")}, found: ${keyValues.map(_.name).mkString(",")}. " +
          "Please review source configuration."
      )

    val valueToHash = keyValues.map(_.value.toString).mkString("#")
    val mergeKey    = HashUtils.murmur3(valueToHash)

    row.filterNot(_.name.equalsIgnoreCase(MergeKeyField.name)) :+ DataCell(
      name = MergeKeyField.name,
      Type = MergeKeyField.fieldType,
      value = mergeKey
    )
