package com.sneaksanddata.arcane.framework
package models.schemas

import models.schemas.ArcaneType.StringType
import services.streaming.base.{JsonWatermark, MetadataEnrichedRowStreamElement, SourceWatermark}

/** Represents a row of data.
  */
type DataRow = List[DataCell]

/** Represents a row of data.
  *
  * @param name
  *   The name of the row.
  * @param Type
  *   The type of the row.
  * @param value
  *   The value of the row.
  */
case class DataCell(name: String, Type: ArcaneType, value: Any)

/** Companion object for [[DataCell]].
  */
object DataCell:
  private val watermarkCellName = "watermark"

  def apply(name: String, Type: ArcaneType, value: Any): DataCell = new DataCell(name, Type, value)
  def watermark(value: String): DataCell = new DataCell(
    watermarkCellName,
    StringType,
    value
  )

  /** Extension method to get the schema of a DataRow.
    */
  extension (row: DataRow)
    def schema: ArcaneSchema =
      row.foldLeft(ArcaneSchema.empty()) {
        case (schema, cell) if cell.name == MergeKeyField.name => schema ++ Seq(MergeKeyField)
        case (schema, cell)                                    => schema ++ Seq(Field(cell.name, cell.Type))
      }

  /** Checks if the cell holds a watermark
    */
  extension (cell: DataCell) def isWatermark: Boolean = cell.name == watermarkCellName

  /** Checks if the row contains a watermark cell
    */
  extension (row: DataRow) def isWatermark: Boolean = row.size == 1 && row.head.isWatermark

  /** Checks if the row contains a watermark cell
   */
  extension (row: DataRow) def getWatermark: Option[String] = row.find(_.isWatermark).map(_.value.toString) 

given NamedCell[DataCell] with
  extension (field: DataCell) def name: String = field.name

given MetadataEnrichedRowStreamElement[DataRow] with
  extension (a: DataRow) def isDataRow: Boolean   = a.isInstanceOf[DataRow]
  extension (a: DataRow) def toDataRow: DataRow   = a
  extension (a: DataRow) def fromDataRow: DataRow = a

object JsonWatermarkRow:
  def apply(watermark: JsonWatermark): DataRow =
    List(
      DataCell.watermark(watermark.toJson)
    )
