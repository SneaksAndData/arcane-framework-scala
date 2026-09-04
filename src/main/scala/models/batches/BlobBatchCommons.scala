package com.sneaksanddata.arcane.framework
package models.batches

import models.schemas.ArcaneType.LongType
import models.schemas.{Field, IndexedField, DataRow, DataCell}

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

  def addVersion(row: DataRow, version: Long): DataRow =
    row :+ DataCell(
      name = BlobBatchCommons.versionField.name,
      Type = BlobBatchCommons.versionField.fieldType,
      value = version
    )

  def enrichBatchRow(row: DataRow, version: Long): DataRow =
    addVersion(row, version)
