package com.sneaksanddata.arcane.framework
package tests.catalog

import models.schemas.ArcaneType.{IntType, StringType}
import models.schemas.{DataCell, Field, MergeKeyField}
import services.iceberg.SchemaConversions.*
import services.iceberg.base.CatalogWriter
import services.iceberg.{IcebergCatalogFactory, IcebergS3CatalogWriter, IcebergStagingEntityManager}
import tests.shared.IcebergCatalogInfo.*
import tests.shared.TestStagingSettings

import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.{Schema, Table}
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Scope, Task, ZIO}

import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.language.postfixOps

object IcebergS3CatalogWriterTests extends ZIOSpecDefault:
  private val schema =
    Seq(MergeKeyField, Field(name = "colA", fieldType = IntType), Field(name = "colB", fieldType = StringType))

  private def getWriter: Task[CatalogWriter[RESTCatalog, Table, Schema]] = ZIO.scoped {
    for
      factory <- IcebergCatalogFactory.live(defaultIcebergStagingSettings)
      entityManager = IcebergStagingEntityManager(defaultIcebergStagingSettings, factory)
      result        = IcebergS3CatalogWriter(entityManager, TestStagingSettings())
    yield result
  }

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("IcebergS3CatalogWriter")(
    test("creates a table when provided schema and rows") {
      for
        writer <- getWriter
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
        result <- writer
          .write(data = rows, name = UUID.randomUUID().toString, schema = schema, logAnnotations = Seq())
          .map { tbl =>
            tbl.currentSnapshot().summary()
          }
      yield assertTrue(result.asScala.getOrElse("total-records", "0").toInt == rows.size)
    },
    test("creates an empty table") {
      for
        writer <- getWriter
        result <- writer
          .write(data = Seq(), name = UUID.randomUUID().toString, schema = schema, logAnnotations = Seq())
          .map(tbl => tbl.history().asScala)
      yield assertTrue(result.nonEmpty)
    },
    test("creates a table and then append rows to it") {
      for
        writer  <- getWriter
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
          .write(data = initialData, name = tblName, schema = schema, logAnnotations = Seq())
          .flatMap(_ => writer.append(appendData, tblName, schema, logAnnotations = Seq()))
          .map { tbl =>
            tbl.currentSnapshot().summary()
          }
      yield assertTrue(result.asScala.getOrElse("total-records", "0").toInt == initialData.size + appendData.size)
    },
    test("creates a table from a large batch") {
      for
        writer  <- getWriter
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
          .write(data = rows, name = tblName, schema = schema, logAnnotations = Seq())
          .map(tbl => tbl.currentSnapshot().summary().asScala.get("added-records"))
      yield assertTrue(result.getOrElse("0").toInt == 20000)
    }
  ) @@ timeout(zio.Duration.fromSeconds(10)) @@ TestAspect.withLiveClock
