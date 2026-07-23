package com.sneaksanddata.arcane.framework
package utils

import models.*
import models.schemas.*

import scala.util.{Failure, Success, Try}

object SqlUtils:

  class JdbcFieldInfo(
      val name: String,
      val typeId: Int,
      val precision: Int,
      val scale: Int
  )
  case class JdbcArrayFieldInfo(
      override val name: String,
      override val typeId: Int,
      arrayBaseElementType: JdbcFieldInfo
  ) extends JdbcFieldInfo(name, typeId, 0, 0)

  case class JdbcRowFieldInfo(
      override val name: String,
      fields: Map[String, JdbcFieldInfo]
  ) extends JdbcFieldInfo(name, java.sql.Types.JAVA_OBJECT, 0, 0)

  given Conversion[JdbcRowFieldInfo, ArcaneSchema]:
    override def apply(x: JdbcRowFieldInfo): ArcaneSchema = x.fields.map { case (fieldName, fieldType) =>
      Field(
        name = fieldName,
        fieldType = toArcaneType(fieldType).get
      )
    }.toSeq

  /** Converts a SQL type to an Arcane type.
    *
    * @param jdbcTypeInfo
    *   The SQL type.
    * @return
    *   The Arcane type.
    */
  def toArcaneType(jdbcTypeInfo: JdbcFieldInfo): Try[ArcaneType] =
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
        jdbcTypeInfo match
          case f: JdbcArrayFieldInfo =>
            toArcaneType(f.arrayBaseElementType).map(elementType => ArcaneType.ListType(elementType, 0))
          case _ =>
            Failure(
              new IllegalArgumentException(
                s"Type of the column ${jdbcTypeInfo.name} has java.sql.types.Array identifier, but is not provided as JdbcArrayFieldInfo"
              )
            )

      case java.sql.Types.JAVA_OBJECT =>
        jdbcTypeInfo match
          case f: JdbcRowFieldInfo => Success(ArcaneType.StructType(f))
          case _ =>
            Failure(
              new IllegalArgumentException(
                s"Type of the column ${jdbcTypeInfo.name} has java.sql.types.JAVA_OBJECT identifier, but is not provided as JdbcRowFieldInfo"
              )
            )
      case _ =>
        Failure(
          new IllegalArgumentException(s"Unsupported SQL type: ${jdbcTypeInfo.typeId} for column ${jdbcTypeInfo.name}")
        )
