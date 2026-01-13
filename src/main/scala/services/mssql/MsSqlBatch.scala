package com.sneaksanddata.arcane.framework
package services.mssql

import models.schemas.{ArcaneType, DataCell, DataRow}
import services.mssql.base.{CanPeekHead, QueryResult}
import services.mssql.query.LazyQueryResult

import java.nio.ByteBuffer
import java.sql.Timestamp
import java.time.{LocalDateTime, OffsetDateTime, ZoneOffset}
import scala.util.{Failure, Try}

case class MsSqlChangeVersion(versionNumber: Long, waterMarkTime: OffsetDateTime)

/** Represents a batch of data.
 */
type MsSqlQueryResult = QueryResult[LazyQueryResult.OutputType] & CanPeekHead[LazyQueryResult.OutputType]

/** Batch type for Microsoft Sql Server is a list of DataRow elements
 */
type MsSqlBatch          = DataRow
type MsSqlVersionedBatch = (DataRow, Long)

extension (row: DataRow)
  private def handleSpecialTypes: DataRow =
    row.map {
      case DataCell(name, ArcaneType.TimestampType, value) if value != null =>
        DataCell(
          name,
          ArcaneType.TimestampType,
          LocalDateTime.ofInstant(value.asInstanceOf[Timestamp].toInstant, ZoneOffset.UTC)
        )

      case DataCell(name, ArcaneType.TimestampType, value) if value == null =>
        DataCell(name, ArcaneType.TimestampType, null)

      case DataCell(name, ArcaneType.ByteArrayType, value) if value != null =>
        DataCell(name, ArcaneType.ByteArrayType, ByteBuffer.wrap(value.asInstanceOf[Array[Byte]]))

      case DataCell(name, ArcaneType.ByteArrayType, value) if value == null =>
        DataCell(name, ArcaneType.ByteArrayType, null)

      case DataCell(name, ArcaneType.ShortType, value) =>
        DataCell(name, ArcaneType.IntType, value.asInstanceOf[Short].toInt)

      case DataCell(name, ArcaneType.DateType, value) =>
        DataCell(name, ArcaneType.DateType, value.asInstanceOf[java.sql.Date].toLocalDate)

      case other => other
    }

