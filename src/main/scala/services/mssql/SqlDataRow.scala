package com.sneaksanddata.arcane.framework
package services.mssql

import models.schemas.{ArcaneType, DataCell, DataRow}
import services.mssql.SqlDataCell.normalizeName

/** Represents a row of data received from the Microsoft SQL Server.
  */
type SqlDataRow = List[SqlDataCell]

/** Represents a cell of data received from the Microsoft SQL Server.
  *
  * @param name
  *   The name of the row.
  * @param Type
  *   The type of the row.
  * @param value
  *   The value of the row.
  */
case class SqlDataCell(name: String, Type: ArcaneType, value: Any)

/** Companion object for [[SqlDataCell]].
  */
object SqlDataCell:
  def apply(name: String, Type: ArcaneType, value: Any): SqlDataCell = new SqlDataCell(name, Type, value)

  /** Normalizes the name of the cell by removing non-word characters.
    *
    * @param name
    *   The name of the cell.
    * @return
    *   The normalized name.
    */
  extension (name: String) def normalizeName: String = "\\W+".r.replaceAllIn(name, "")

given Conversion[SqlDataRow, DataRow] with
  override def apply(dataRow: SqlDataRow): DataRow =
    dataRow.map { cell =>
      DataCell(cell.name.normalizeName, cell.Type, cell.value)
    }
