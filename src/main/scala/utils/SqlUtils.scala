package com.sneaksanddata.arcane.framework
package utils

import models.*
import models.schemas.*

import java.sql.{JDBCType, ResultSet}
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

  private def parseDecimalType(decimalTypeString: String): JdbcFieldInfo = new JdbcFieldInfo(
    name = "",
    typeId = java.sql.Types.DECIMAL,
    precision = decimalTypeString.split(",").head.stripPrefix("decimal(").toInt,
    scale = decimalTypeString.split(",").reverse.head.stripSuffix(")").toInt
  )

  private def parseArrayType(arrayTypeString: String): JdbcFieldInfo = {
    arrayTypeString.stripPrefix("array(").stripSuffix(")") match {
      case "varchar" =>
        new JdbcFieldInfo(
          name = "",
          typeId = java.sql.Types.VARCHAR,
          precision = 0,
          scale = 0
        )
      case "integer" =>
        new JdbcFieldInfo(
          name = "",
          typeId = java.sql.Types.INTEGER,
          precision = 0,
          scale = 0
        )
      case "float" =>
        new JdbcFieldInfo(
          name = "",
          typeId = java.sql.Types.FLOAT,
          precision = 0,
          scale = 0
        )
      case "double" =>
        new JdbcFieldInfo(
          name = "",
          typeId = java.sql.Types.DOUBLE,
          precision = 0,
          scale = 0
        )
      case decimal if decimal.startsWith("decimal") => parseDecimalType(decimal)
      // rows are currently just objects, until https://github.com/trinodb/trino/issues/16479
      case row if row.startsWith("row") => parseRowType(row)
      case _ => throw new RuntimeException(s"Unmapped array type for schema migration: $arrayTypeString")
    }
  }

  private def parseRowType(rowTypeString: String, fieldName: Option[String] = None): JdbcRowFieldInfo =
    JdbcRowFieldInfo(
      // row(a varchar,b integer,c row(a varchar,c integer),d integer)
      name = fieldName.getOrElse(""),
      fields = rowTypeString
        .stripPrefix("row")
        .stripPrefix("(")
        .stripSuffix(")")
        .split(",")
        .foldLeft(List.empty[String]) { case (agg, e) =>
          if e.contains(")") && agg.nonEmpty then // in case we have a nested ROW
            agg.init :+ agg.last.concat(s",$e")
          else agg ++ Seq(e)
        }
        .map { typeString =>
          val (typeName, typeVal) = typeString.split(" ", 2).toList match
            case tpn :: tpv :: _ => (tpn, tpv)
            case _ =>
              throw new RuntimeException(
                s"Cannot resolve types for schema migration: invalid type string for ROW type received from JDBC client: $typeString"
              )

          typeVal match
            case row if row.startsWith("row")             => typeName -> parseRowType(row)
            case decimal if decimal.startsWith("decimal") => typeName -> parseDecimalType(decimal)
            case array if array.startsWith("array")       => typeName -> parseArrayType(array)
            case _ =>
              typeName -> JdbcFieldInfo(
                name = "",
                typeId = JDBCType.valueOf(typeVal.toUpperCase).getVendorTypeNumber,
                precision = 0,
                scale = 0
              )
        }
        .toMap
    )

  /** Gets the columns of a result set.
    */
  extension (resultSet: ResultSet)
    def getColumns: Seq[JdbcFieldInfo] =
      for i <- 1 to resultSet.getMetaData.getColumnCount
      yield
        if resultSet.getMetaData.getColumnType(i) == java.sql.Types.ARRAY then
          JdbcArrayFieldInfo(
            name = resultSet.getMetaData.getColumnName(i),
            typeId = resultSet.getMetaData.getColumnType(i),
            parseArrayType(resultSet.getMetaData.getColumnTypeName(i))
          )
        else if resultSet.getMetaData.getColumnType(i) == java.sql.Types.JAVA_OBJECT then
          parseRowType(
            resultSet.getMetaData.getColumnTypeName(i),
            fieldName = Some(resultSet.getMetaData.getColumnName(i))
          )
        else
          new JdbcFieldInfo(
            name = resultSet.getMetaData.getColumnName(i),
            typeId = resultSet.getMetaData.getColumnType(i),
            precision = resultSet.getMetaData.getPrecision(i),
            scale = resultSet.getMetaData.getScale(i)
          )
