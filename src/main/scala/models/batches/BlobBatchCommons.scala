package com.sneaksanddata.arcane.framework
package models.batches

import exceptions.FatalStreamFailException
import models.schemas.ArcaneType.LongType
import models.schemas.*

import java.security.MessageDigest
import java.util.Base64

object BlobBatchCommons:
  val versionField: Field = Field(
    name = "createdon",
    fieldType = LongType
  )
  def indexedVersionField(id: Int): IndexedField = IndexedField(
    name = versionField.name,
    fieldType = versionField.fieldType,
    fieldId = id
  )

  private def encodeHash(hash: Array[Byte]): String = Base64.getEncoder.encodeToString(hash)

  private def getMergeKeyValue(row: DataRow, keys: Seq[String], hasher: MessageDigest): String = encodeHash(
    hasher.digest(
      keys
        .map { key =>
          row.find(cell => cell.name == key) match
            case Some(pkCell) => pkCell.value.toString
            case None =>
              throw FatalStreamFailException(s"Primary key $key does not exist in the rows emitted by this source")
        }
        .mkString
        .toLowerCase
        .getBytes("UTF-8")
    )
  )

  def addVersion(row: DataRow, version: Long): DataRow =
    row :+ DataCell(
      name = BlobBatchCommons.versionField.name,
      Type = BlobBatchCommons.versionField.fieldType,
      value = version
    )

  def addLegacyMergeKey(row: DataRow, primaryKeys: Seq[String], hasher: MessageDigest): DataRow =
    row :+ DataCell(
      name = MergeKeyField.name,
      Type = MergeKeyField.fieldType,
      value = getMergeKeyValue(row, primaryKeys, hasher)
    )

  def enrichBatchRow(
      row: DataRow,
      version: Long,
      primaryKeys: Seq[String],
      hasher: MessageDigest,
      includeLegacyMergeKey: Boolean
  ): DataRow =
    val rowWithLegacyMergeKey =
      if includeLegacyMergeKey then addLegacyMergeKey(row, primaryKeys, hasher)
      else row
    addVersion(rowWithLegacyMergeKey, version)
