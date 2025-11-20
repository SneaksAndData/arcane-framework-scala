package com.sneaksanddata.arcane.framework
package models.batches

import models.schemas.ArcaneType.LongType
import models.schemas.{DataCell, DataRow, Field, MergeKeyField}

import java.security.MessageDigest
import java.util.Base64

object BlobBatchCommons:
  val versionField: Field = Field(
    name = "createdon",
    fieldType = LongType
  )

  private val mergeKeyHasher = MessageDigest.getInstance("SHA-256")

  private def encodeHash(hash: Array[Byte]): String = Base64.getEncoder.encodeToString(hash)

  private def getMergeKeyValue(row: DataRow, keys: Seq[String]): String = encodeHash(
    mergeKeyHasher.digest(
      keys
        .map { key =>
          row.find(cell => cell.name == key) match
            case Some(pkCell) => pkCell.value.toString
            case None =>
              throw new RuntimeException(s"Primary key $key does not exist in the rows emitted by this source")
        }
        .mkString
        .toLowerCase
        .getBytes("UTF-8")
    )
  )

  def enrichBatchRow(row: DataRow, version: Long, primaryKeys: Seq[String]): DataRow = row ++ Seq(
    DataCell(
      name = MergeKeyField.name,
      Type = MergeKeyField.fieldType,
      value = getMergeKeyValue(row, primaryKeys)
    ),
    // merge query requires a versionField to ensure rows are updated correctly
    DataCell(
      name = BlobBatchCommons.versionField.name,
      Type = BlobBatchCommons.versionField.fieldType,
      value = version
    )
  )
