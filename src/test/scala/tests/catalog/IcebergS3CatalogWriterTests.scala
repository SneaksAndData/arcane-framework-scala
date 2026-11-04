package com.sneaksanddata.arcane.framework
package tests.catalog

import models.schemas.ArcaneType.{IntType, StringType}
import models.schemas.{ArcaneSchema, DataCell, Field, LoadTimestampField, MergeKeyField}
import models.settings.sources.modification.{DataRowModification, FrozenSurrogateTimestamp}
import services.base.DefaultStreamingSource
import services.iceberg.SchemaConversions.*
import services.iceberg.{IcebergCatalogFactory, IcebergS3CatalogWriter, IcebergStagingEntityManager}
import tests.shared.IcebergCatalogInfo.*
import tests.shared.TestStagingSettings

import org.apache.iceberg.SnapshotChanges
import org.apache.iceberg.data.Record
import org.apache.iceberg.data.parquet.GenericParquetReaders
import org.apache.iceberg.parquet.Parquet
import zio.test.*
import zio.test.TestAspect.timeout
import zio.{Chunk, Scope, Task, ZIO, ZLayer}

import java.time.{LocalDateTime, ZoneOffset}
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.language.postfixOps
import scala.util.Using

object IcebergS3CatalogWriterTests extends ZIOSpecDefault:
  private val schema =
    Seq(MergeKeyField, Field(name = "colA", fieldType = IntType), Field(name = "colB", fieldType = StringType))

  private class TestSource(mods: Seq[DataRowModification]) extends DefaultStreamingSource(mods):
    override type ShardMetadata = String
    override type WatermarkType = String

    override protected def getSourceSchema: Task[ArcaneSchema] = ZIO.succeed(schema)

    override def getShards(rangeStart: WatermarkType, rangeEnd: WatermarkType) = zio.stream.ZStream.empty

    override def deleteShards(prefix: String): Task[Unit] = ZIO.unit

    override def empty: ArcaneSchema = Seq.empty

  private val writerLayer: ZLayer[Any, Throwable, IcebergS3CatalogWriter] = ZLayer.scoped {
    for
      factory <- IcebergCatalogFactory.live(defaultIcebergStagingSettings)
      entityManager = IcebergStagingEntityManager(defaultIcebergStagingSettings, factory)
      result        = IcebergS3CatalogWriter(entityManager, TestStagingSettings())
    yield result
  }

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("IcebergS3CatalogWriter")(
    test("creates a table when provided schema and rows") {
      for
        writer <- ZIO.service[IcebergS3CatalogWriter]
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
    test("writes a row with a frozen surrogate load timestamp") {
      val frozenTimestamp = LocalDateTime.of(2026, 9, 4, 12, 30)
      val modification    = FrozenSurrogateTimestamp(frozenTimestamp.atOffset(ZoneOffset.UTC))
      val source          = new TestSource(Seq(modification))
      for
        writer       <- ZIO.service[IcebergS3CatalogWriter]
        sourceSchema <- source.getSchema
        rows = source.applyDataRowModifications(
          Chunk(
            List(
              DataCell(name = MergeKeyField.name, Type = MergeKeyField.fieldType, value = "key1"),
              DataCell(name = "colA", Type = IntType, value = 1),
              DataCell(name = "colB", Type = StringType, value = "abc")
            )
          ),
          Seq(modification)
        )
        tbl <- writer.write(
          data = rows,
          name = UUID.randomUUID().toString,
          schema = sourceSchema,
          logAnnotations = Seq()
        )
        written <- ZIO.attemptBlocking {
          val file =
            SnapshotChanges.builderFor(tbl).snapshot(tbl.currentSnapshot()).build().addedDataFiles().iterator().next()
          Using.resource(
            Parquet
              .read(tbl.io().newInputFile(file.location()))
              .project(tbl.schema())
              .createReaderFunc(fileSchema => GenericParquetReaders.buildReader(tbl.schema(), fileSchema))
              .build[Record]()
          )(_.iterator().asScala.toList)
        }
      yield assertTrue(
        written.size == 1,
        written.head.getField(MergeKeyField.name) == "key1",
        written.head.getField("colA") == 1,
        written.head.getField("colB") == "abc",
        written.head.getField(LoadTimestampField.name) == frozenTimestamp
      )
    },
    test("creates an empty table") {
      for
        writer <- ZIO.service[IcebergS3CatalogWriter]
        result <- writer
          .write(data = Seq(), name = UUID.randomUUID().toString, schema = schema, logAnnotations = Seq())
          .map(tbl => tbl.history().asScala)
      yield assertTrue(result.nonEmpty)
    },
    test("creates a table and then append rows to it") {
      for
        writer  <- ZIO.service[IcebergS3CatalogWriter]
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
        writer  <- ZIO.service[IcebergS3CatalogWriter]
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
  ).provideLayer(writerLayer) @@ timeout(zio.Duration.fromSeconds(10)) @@ TestAspect.withLiveClock
