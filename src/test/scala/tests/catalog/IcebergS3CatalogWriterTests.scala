package com.sneaksanddata.arcane.framework
package tests.catalog

import models.schemas.ArcaneType.{IntType, StringType}
import models.schemas.{DataCell, Field, MergeKeyField}
import services.iceberg.IcebergS3CatalogWriter
import services.iceberg.SchemaConversions.*
import services.iceberg.base.CatalogWriter
import tests.shared.IcebergCatalogInfo.*

import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, ZIO}

import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.language.postfixOps

object IcebergS3CatalogWriterTests extends ZIOSpecDefault:
  private val schema =
    Seq(MergeKeyField, Field(name = "colA", fieldType = IntType), Field(name = "colB", fieldType = StringType))
  private val writer: CatalogWriter[RESTCatalog, Table, Schema] = IcebergS3CatalogWriter(defaultStagingSettings)

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("IcebergS3CatalogWriter")(
    test("creates a table when provided schema and rows") {
      for
        rows <- ZIO.succeed(
          Seq(
            List(
              DataCell(name = MergeKeyField.name, Type = MergeKeyField.fieldType, value = "key1"),
              DataCell(name = "colA", Type = IntType, value = 1),
              DataCell(name = "colB", Type = StringType, value = "abc")
            ),
            List(
              DataCell(name = MergeKeyField.name, Type = MergeKeyField.fieldType, value = "key2"),
              DataCell(name = "colA", Type = IntType, value = 2),
              DataCell(name = "colB", Type = StringType, value = "def")
            ),
            List(
              DataCell(name = MergeKeyField.name, Type = MergeKeyField.fieldType, value = "key3"),
              DataCell(name = "colA", Type = IntType, value = 2),
              DataCell(name = "colB", Type = StringType, value = "iop")
            ),
            List(
              DataCell(name = MergeKeyField.name, Type = MergeKeyField.fieldType, value = "key4"),
              DataCell(name = "colA", Type = IntType, value = 3),
              DataCell(name = "colB", Type = StringType, value = "tyr")
            )
          )
        )
        result <- writer.write(data = rows, name = UUID.randomUUID().toString, schema = schema).map { tbl =>
          tbl.currentSnapshot().summary()
        }
      yield assertTrue(result.asScala.getOrElse("total-records", "0").toInt == rows.size)
    },
    test("creates an empty table") {
      for result <- writer
          .write(data = Seq(), name = UUID.randomUUID().toString, schema = schema)
          .map(tbl => tbl.history().asScala)
      yield assertTrue(result.nonEmpty)
    },
    test("deletes a table successfully after creating it") {
      for
        tblName <- ZIO.succeed(UUID.randomUUID.toString)
        rows <- ZIO.succeed(
          Seq(
            List(
              DataCell(name = MergeKeyField.name, Type = MergeKeyField.fieldType, value = "key1"),
              DataCell(name = "colA", Type = IntType, value = 1),
              DataCell(name = "colB", Type = StringType, value = "abc")
            )
          )
        )
        result <- writer.write(data = rows, name = tblName, schema = schema).flatMap(_ => writer.delete(tblName))
      yield assertTrue(result)
    },
    test("creates a table and then append rows to it") {
      for
        tblName <- ZIO.succeed(UUID.randomUUID.toString)
        initialData <- ZIO.succeed(
          Seq(
            List(
              DataCell(name = MergeKeyField.name, Type = MergeKeyField.fieldType, value = "key1"),
              DataCell(name = "colA", Type = IntType, value = 1),
              DataCell(name = "colB", Type = StringType, value = "abc")
            )
          )
        )
        appendData <- ZIO.succeed(
          Seq(
            List(
              DataCell(name = MergeKeyField.name, Type = MergeKeyField.fieldType, value = "key2"),
              DataCell(name = "colA", Type = IntType, value = 2),
              DataCell(name = "colB", Type = StringType, value = "def")
            )
          )
        )
        result <- writer
          .write(data = initialData, name = tblName, schema = schema)
          .flatMap(_ => writer.append(appendData, tblName, schema))
          .map { tbl =>
            tbl.currentSnapshot().summary()
          }
      yield assertTrue(result.asScala.getOrElse("total-records", "0").toInt == initialData.size + appendData.size)
    },
    test("creates a table from a large batch") {
      for
        tblName <- ZIO.succeed(UUID.randomUUID.toString)
        rows <- ZIO.succeed(
          Range(0, 20000).map(index =>
            List(
              DataCell(name = MergeKeyField.name, Type = MergeKeyField.fieldType, value = s"key$index"),
              DataCell(name = "colA", Type = IntType, value = index),
              DataCell(name = "colB", Type = StringType, value = s"abc$index")
            )
          )
        )
        result <- writer
          .write(data = rows, name = tblName, schema = schema)
          .map(tbl => tbl.currentSnapshot().summary().asScala.get("added-records"))
      yield assertTrue(result.getOrElse("0").toInt == 20000)
    }
  ) @@ timeout(zio.Duration.fromSeconds(10)) @@ TestAspect.withLiveClock
