package com.sneaksanddata.arcane.framework
package tests.sql

import utils.SqlUtils.*

import io.trino.jdbc.TrinoResultSet
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.should

import java.sql.{Connection, DriverManager}

class SqlUtilsTests extends AnyFlatSpec with Matchers {
  it should "parse JDBC metadata into Arcane Types" in {
    val sqlConnection: Connection = DriverManager.getConnection("jdbc:trino://localhost:8080/iceberg/test?user=test")
    val createTableStatement = sqlConnection.prepareStatement(
      "create table if not exists iceberg.test.array_table (c1 integer, c2 varchar, c3 array(varchar), c4 array(decimal(16, 3)))"
    )
    createTableStatement.execute()

    val columns =
      sqlConnection.prepareStatement("select * from iceberg.test.array_table where 1=0").executeQuery().getColumns

    (
      columns.size should be(4),
      columns
        .find(c => c.name == "c3")
        .map { case c: JdbcArrayFieldInfo =>
          c.typeId == java.sql.Types.ARRAY && c.arrayBaseElementType.typeId == java.sql.Types.VARCHAR
        }
        .isDefined,
      columns
        .find(c => c.name == "c4")
        .map { case c: JdbcArrayFieldInfo =>
          c.typeId == java.sql.Types.ARRAY && c.arrayBaseElementType.typeId == java.sql.Types.DECIMAL && c.arrayBaseElementType.precision == 16 && c.arrayBaseElementType.scale == 3
        }
        .isDefined
    )
  }
}
