package com.sneaksanddata.arcane.framework
package utils

import models.*

import java.sql.ResultSet
import scala.util.{Failure, Success, Try}

object SqlUtils:

  /**
   * Reads the schema of a table from a SQL result set.
   *
   * @param resultSet The result set.
   * @return The schema of the table.
   */
  extension (resultSet: ResultSet) def readArcaneSchema: Try[ArcaneSchema] =
    val columns = resultSet.getColumns.map({ case (name, sqlType, precision, scale) =>
      (name.toUpperCase(), toArcaneType(sqlType, precision, scale))
    })
    val arcaneColumns = for c <- columns
      yield c match
        case (MergeKeyField.name, Success(_)) => Success(MergeKeyField)
        case (DatePartitionField.name, Success(_)) => Success(DatePartitionField)
        case (name, Success(arcaneType)) => Success(Field(name, arcaneType))
        case (_, Failure(e)) => Failure(e)
        
    Try(arcaneColumns.collect{
      case Success(field) => field
      case Failure(e) => throw e
    })
        
  /**
   * Converts a SQL type to an Arcane type.
   *
   * @param sqlType The SQL type.
   * @return The Arcane type.
   */
  def toArcaneType(sqlType: Int, precision: Int, scale: Int): Try[ArcaneType] = sqlType match
    case java.sql.Types.BIGINT => Success(ArcaneType.LongType)
    case java.sql.Types.BINARY => Success(ArcaneType.ByteArrayType)
    case java.sql.Types.BIT => Success(ArcaneType.BooleanType)
    case java.sql.Types.BOOLEAN => Success(ArcaneType.BooleanType)
    case java.sql.Types.CHAR => Success(ArcaneType.StringType)
    case java.sql.Types.DATE => Success(ArcaneType.DateType)
    case java.sql.Types.TIMESTAMP => Success(ArcaneType.TimestampType)
    case java.sql.Types.TIMESTAMP_WITH_TIMEZONE => Success(ArcaneType.DateTimeOffsetType)
    case java.sql.Types.DECIMAL => Success(ArcaneType.BigDecimalType(precision, scale))

    // numeric is functionally identical to decimal
    // see: https://learn.microsoft.com/en-us/sql/t-sql/data-types/decimal-and-numeric-transact-sql
    case java.sql.Types.NUMERIC => Success(ArcaneType.BigDecimalType(precision, scale))

    // The SQL Server text and ntext types map to the JDBC LONGVARCHAR and LONGNVARCHAR type, respectively.
    // see: https://learn.microsoft.com/en-us/sql/connect/jdbc/understanding-data-type-differences?view=sql-server-ver16#character-types
    case java.sql.Types.LONGVARCHAR => Success(ArcaneType.StringType)
    case java.sql.Types.LONGNVARCHAR => Success(ArcaneType.StringType)
    case java.sql.Types.DOUBLE => Success(ArcaneType.DoubleType)
    case java.sql.Types.INTEGER => Success(ArcaneType.IntType)

    case java.sql.Types.FLOAT => Success(ArcaneType.FloatType)
    // The ISO synonym for real is float(24).
    // See: https://learn.microsoft.com/en-us/sql/t-sql/data-types/float-and-real-transact-sql?view=sql-server-ver16
    case java.sql.Types.REAL => Success(ArcaneType.FloatType)
    
    case java.sql.Types.SMALLINT => Success(ArcaneType.ShortType)
    case java.sql.Types.TIME => Success(ArcaneType.TimeType)
    case java.sql.Types.NCHAR => Success(ArcaneType.StringType)
    case java.sql.Types.NVARCHAR => Success(ArcaneType.StringType)
    case java.sql.Types.VARCHAR => Success(ArcaneType.StringType)
    case java.sql.Types.VARBINARY => Success(ArcaneType.ByteArrayType)
    case _ => Failure(new IllegalArgumentException(s"Unsupported SQL type: $sqlType"))


  /**
   * Gets the columns of a result set.
   */
  extension (resultSet: ResultSet) def getColumns: Seq[(String, Int, Int, Int)] =
    for i <- 1 to resultSet.getMetaData.getColumnCount
      yield (resultSet.getMetaData.getColumnName(i),
        resultSet.getMetaData.getColumnType(i),
        resultSet.getMetaData.getPrecision(i),
        resultSet.getMetaData.getScale(i))
