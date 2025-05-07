package com.sneaksanddata.arcane.framework
package models.schemas

import services.streaming.base.MetadataEnrichedRowStreamElement

/**
 * Represents a row of data.
 */
type DataRow = List[DataCell]

/**
 * Represents a row of data.
 *
 * @param name The name of the row.
 * @param Type The type of the row.
 * @param value The value of the row.
 */
case class DataCell(name: String, Type: ArcaneType, value: Any)

/**
 * Companion object for [[DataCell]].
 */
object DataCell:
  def apply(name: String, Type: ArcaneType, value: Any): DataCell = new DataCell(name, Type, value)

  /**
   * Extension method to get the schema of a DataRow.
   */
  extension (row: DataRow) def schema: ArcaneSchema =
    row.foldLeft(ArcaneSchema.empty()) {
      case (schema, cell) if cell.name == MergeKeyField.name => schema ++ Seq(MergeKeyField)
      case (schema, cell) if cell.name == DatePartitionField.name => schema ++ Seq(DatePartitionField)
      case (schema, cell) => schema ++ Seq(Field(cell.name, cell.Type))
    }

given NamedCell[DataCell] with
  extension (field: DataCell) def name: String = field.name
  
given MetadataEnrichedRowStreamElement[DataRow] with
  extension (a: DataRow) def isDataRow: Boolean = a.isInstanceOf[DataRow]
  extension (a: DataRow) def toDataRow: DataRow = a
  extension (a: DataRow) def fromDataRow: DataRow  = a
