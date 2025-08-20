package com.sneaksanddata.arcane.framework
package utils

import models.*
import models.schemas.*

import java.sql.ResultSet
import scala.util.{Failure, Success, Try}

object SqlUtils:

  class JdbcTypeInfo(
      val name: String,
      val typeId: Int,
      val precision: Int,
      val scale: Int
  )
  private case class JdbcArrayTypeInfo(
      name: String,
      typeId: Int,
      arrayBaseElementType: JdbcTypeInfo
  ) extends JdbcTypeInfo(name, typeId, 0, 0)

  /** Reads the schema of a table from a SQL result set.
    *
    * @param resultSet
    *   The result set.
    * @return
    *   The schema of the table.
    */
  extension (resultSet: ResultSet)
    def readArcaneSchema: Try[ArcaneSchema] =
      val columns = resultSet.getColumns.map({ typeInfo =>
        (
          typeInfo.name.toUpperCase(),
          toArcaneType(typeInfo)
        )
      })
      val arcaneColumns =
        for c <- columns
        yield c match
          case (MergeKeyField.name, Success(_)) => Success(MergeKeyField)
          case (name, Success(arcaneType))      => Success(Field(name, arcaneType))
          case (_, Failure(e))                  => Failure(e)

      Try(arcaneColumns.collect {
        case Success(field) => field
        case Failure(e)     => throw e
      })

  /** Converts a SQL type to an Arcane type.
    *
    * @param jdbcTypeInfo
    *   The SQL type.
    * @return
    *   The Arcane type.
    */
  def toArcaneType(jdbcTypeInfo: JdbcTypeInfo): Try[ArcaneType] =
    jdbcTypeInfo.typeId match
      case java.sql.Types.BIGINT                  => Success(ArcaneType.LongType)
      case java.sql.Types.BINARY                  => Success(ArcaneType.ByteArrayType)
      case java.sql.Types.BIT                     => Success(ArcaneType.BooleanType)
      case java.sql.Types.BOOLEAN                 => Success(ArcaneType.BooleanType)
      case java.sql.Types.CHAR                    => Success(ArcaneType.StringType)
      case java.sql.Types.DATE                    => Success(ArcaneType.DateType)
      case java.sql.Types.TIMESTAMP               => Success(ArcaneType.TimestampType)
      case java.sql.Types.TIMESTAMP_WITH_TIMEZONE => Success(ArcaneType.DateTimeOffsetType)
      case java.sql.Types.DECIMAL => Success(ArcaneType.BigDecimalType(jdbcTypeInfo.precision, jdbcTypeInfo.scale))

      // numeric is functionally identical to decimal
      // see: https://learn.microsoft.com/en-us/sql/t-sql/data-types/decimal-and-numeric-transact-sql
      case java.sql.Types.NUMERIC => Success(ArcaneType.BigDecimalType(jdbcTypeInfo.precision, jdbcTypeInfo.scale))

      // The SQL Server text and ntext types map to the JDBC LONGVARCHAR and LONGNVARCHAR type, respectively.
      // see: https://learn.microsoft.com/en-us/sql/connect/jdbc/understanding-data-type-differences?view=sql-server-ver16#character-types
      case java.sql.Types.LONGVARCHAR  => Success(ArcaneType.StringType)
      case java.sql.Types.LONGNVARCHAR => Success(ArcaneType.StringType)
      case java.sql.Types.DOUBLE       => Success(ArcaneType.DoubleType)
      case java.sql.Types.INTEGER      => Success(ArcaneType.IntType)

      case java.sql.Types.FLOAT => Success(ArcaneType.FloatType)
      // The ISO synonym for real is float(24).
      // See: https://learn.microsoft.com/en-us/sql/t-sql/data-types/float-and-real-transact-sql?view=sql-server-ver16
      case java.sql.Types.REAL => Success(ArcaneType.FloatType)

      case java.sql.Types.SMALLINT  => Success(ArcaneType.ShortType)
      case java.sql.Types.TINYINT   => Success(ArcaneType.ShortType)
      case java.sql.Types.TIME      => Success(ArcaneType.TimeType)
      case java.sql.Types.NCHAR     => Success(ArcaneType.StringType)
      case java.sql.Types.NVARCHAR  => Success(ArcaneType.StringType)
      case java.sql.Types.VARCHAR   => Success(ArcaneType.StringType)
      case java.sql.Types.VARBINARY => Success(ArcaneType.ByteArrayType)
      case java.sql.Types.ARRAY =>
        jdbcTypeInfo match {
          case f: JdbcArrayTypeInfo =>
            toArcaneType(f.arrayBaseElementType).map(elementType => ArcaneType.ListType(elementType, 0))
          case _ =>
            Failure(
              new IllegalArgumentException(
                s"Type ${jdbcTypeInfo.name} has java.sql.types.Array identifier, but is not provided as JdbcArrayTypeInfo"
              )
            )
        }

      case _ => Failure(new IllegalArgumentException(s"Unsupported SQL type: ${jdbcTypeInfo.typeId}"))

  /** Gets the columns of a result set.
    */
  extension (resultSet: ResultSet)
    def getColumns: Seq[JdbcTypeInfo] =
      for i <- 1 to resultSet.getMetaData.getColumnCount
      yield
        if resultSet.getMetaData.getColumnType(i) == java.sql.Types.ARRAY then
          JdbcArrayTypeInfo(
            name = resultSet.getMetaData.getColumnName(i),
            typeId = resultSet.getMetaData.getColumnType(i),
            new JdbcTypeInfo(
              name = "",
              typeId = resultSet.getArray(i).getBaseType,
              precision = resultSet.getArray(i).getResultSet.getMetaData.getPrecision(1),
              scale = resultSet.getArray(i).getResultSet.getMetaData.getScale(1)
            )
          )
        else
          new JdbcTypeInfo(
            name = resultSet.getMetaData.getColumnName(i),
            typeId = resultSet.getMetaData.getColumnType(i),
            precision = resultSet.getMetaData.getPrecision(i),
            scale = resultSet.getMetaData.getScale(i)
          )
