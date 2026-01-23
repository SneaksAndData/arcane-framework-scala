package com.sneaksanddata.arcane.framework
package tests.mssql

import models.schemas.ArcaneType.*
import models.schemas.{ArcaneSchemaField, DataCell, Field, MergeKeyField}
import models.settings.FieldSelectionRule.{ExcludeFields, IncludeFields}
import models.settings.{FieldSelectionRule, FieldSelectionRuleSettings}
import services.filters.ColumnSummaryFieldsFilteringService
import services.mssql.QueryProvider
import services.mssql.base.{ColumnSummary, ConnectionOptions, MsSqlReader, MsSqlServerFieldsFilteringService}
import services.mssql.versioning.MsSqlWatermark
import tests.mssql.util.MsSqlTestServices.*

import org.scalatest.*
import org.scalatest.matchers.should.Matchers.*
import zio.test.*
import zio.test.Assertion.{equalTo, fails, hasMessage}
import zio.test.TestAspect.timeout
import zio.{Scope, Task, ZIO}

import java.sql.Connection
import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, OffsetDateTime, ZoneOffset}
import scala.List
import scala.language.postfixOps
import scala.util.Success

object MsSqlReaderTests extends ZIOSpecDefault:
  private implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  private val fieldString = "(x int not null, y int, z DECIMAL(30, 6), a VARBINARY(MAX), b DATETIME, [c/d] int, e real)"
  private val pkString    = "primary key(x)"
  private val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  private val emptyFieldsFilteringService: MsSqlServerFieldsFilteringService = (fields: List[ColumnSummary]) =>
    Success(fields)

  def insertData(con: Connection, tableName: String): Task[Unit] =
    for
      _ <- ZIO.acquireReleaseWith(ZIO.attempt(con.createStatement()))(statement =>
        ZIO.attemptBlocking(statement.close()).orDie
      ) { statement =>
        ZIO.foreach(1 to 10) { index =>
          val insertCmd =
            s"use arcane; insert into dbo.$tableName values($index, ${index + 1}, null, CAST(123456 AS VARBINARY(MAX)), '2023-10-01 12:34:56', 0, 0)"
          ZIO.attemptBlocking(statement.execute(insertCmd))
        }
      }
      _ <- ZIO.acquireReleaseWith(ZIO.attempt(con.createStatement()))(statement =>
        ZIO.attemptBlocking(statement.close()).orDie
      ) { statement =>
        ZIO.foreach(1 to 10) { index =>
          val updateCmd =
            s"use arcane; insert into dbo.$tableName values(${index * 1000}, ${index * 1000 + 1}, ${index * 1000 + 2}, CAST(123456 AS VARBINARY(MAX)), '2023-10-01 12:34:56', 0, 0)"
          ZIO.attemptBlocking(statement.execute(updateCmd))
        }
      }
    yield ()

  def updateData(con: Connection, tableName: String): Task[Unit] =
    for _ <- ZIO.acquireReleaseWith(ZIO.attempt(con.createStatement()))(statement =>
        ZIO.attemptBlocking(statement.close()).orDie
      ) { statement =>
        ZIO.foreach(1 to 10) { index =>
          val updateCmd =
            s"use arcane; update dbo.$tableName set y = ${index * scala.util.Random.nextInt()};"
          ZIO.attemptBlocking(statement.execute(updateCmd))
        }
      }
    yield ()

  def deleteData(connection: Connection, primaryKeys: Seq[Int], tableName: String): ZIO[Any, Throwable, Unit] =
    ZIO.scoped {
      for
        statement <- ZIO.attempt(connection.prepareStatement(s"use arcane; delete from dbo.$tableName where x = ?"))
        _ <- ZIO.foreachDiscard(primaryKeys) { number =>
          ZIO.attempt {
            statement.setInt(1, number)
            statement.executeUpdate()
          }
        }
      yield ()
    }

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("MsSqlConnectionTests")(
    test("QueryProvider generates columns query") {
      for
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection => ZIO.attemptBlocking(createTable("columns_query_test", connection, fieldString, pkString))
        )
        connector <- ZIO.succeed(
          MsSqlReader(
            ConnectionOptions(connectionUrl, "dbo", "columns_query_test", None),
            emptyFieldsFilteringService
          )
        )
        query <- QueryProvider.getColumnSummariesQuery(
          connector.connectionOptions.schemaName,
          connector.connectionOptions.tableName,
          connector.catalog
        )
      yield assertTrue(query.contains("case when kcu.CONSTRAINT_NAME is not null then 1 else 0 end as IsPrimaryKey"))
    },
    test("QueryProvider generates schema query") {
      for
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection => ZIO.attemptBlocking(createTable("schema_query_test", connection, fieldString, pkString))
        )
        connector <- ZIO.succeed(
          MsSqlReader(
            ConnectionOptions(connectionUrl, "dbo", "schema_query_test", None),
            emptyFieldsFilteringService
          )
        )
        query <- QueryProvider.getSchemaQuery(connector)
      yield assertTrue(query.contains("ct.SYS_CHANGE_VERSION") && query.contains("ARCANE_MERGE_KEY"))
    },
    test("QueryProvider generates time-based query") {
      for
        currentTime <- ZIO.succeed(OffsetDateTime.ofInstant(Instant.now().minus(Duration.ofHours(-1)), ZoneOffset.UTC))
        query       <- ZIO.succeed(QueryProvider.getVersionFromTimestampQuery(currentTime, formatter))
        formatted   <- ZIO.succeed(formatter.format(currentTime))
      yield assertTrue(
        query.contains("SELECT MIN(commit_ts)") && query.contains(s"WHERE commit_time >= '$formatted'")
      )
    },
    test("QueryProvider generates backfill query") {
      for
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection => ZIO.attemptBlocking(createTable("backfill_query", connection, fieldString, pkString))
        )
        connector <- ZIO.succeed(
          MsSqlReader(
            ConnectionOptions(connectionUrl, "dbo", "backfill_query", None),
            emptyFieldsFilteringService
          )
        )
        expected <- ZIO.succeed("""declare @currentVersion bigint = CHANGE_TRACKING_CURRENT_VERSION()
            |
            |SELECT
            |tq.[x],
            |CAST(0 as BIGINT) as SYS_CHANGE_VERSION,
            |'I' as SYS_CHANGE_OPERATION,
            |tq.[y],
            |tq.[z],
            |tq.[a],
            |tq.[b],
            |tq.[c/d],
            |tq.[e],
            |@currentVersion AS 'ChangeTrackingVersion',
            |lower(convert(nvarchar(128), HashBytes('SHA2_256', cast(tq.[x] as nvarchar(128))),2)) as [ARCANE_MERGE_KEY]
            |FROM [arcane].[dbo].[backfill_query] tq""".stripMargin)
        query <- QueryProvider.getBackfillQuery(connector)
      yield assertTrue(query == expected)
    },
    test("QueryProvider handles field selection rule") {
      for
        fieldSelectionRule <- ZIO.succeed(new FieldSelectionRuleSettings {
          override val rule: FieldSelectionRule = ExcludeFields(Set("b", "a", "z", "cd"))
          override val essentialFields: Set[String] =
            Set("SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION", "ARCANE_MERGE_KEY", "ChangeTrackingVersion")
          override val isServerSide: Boolean = true
        })
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection => ZIO.attemptBlocking(createTable("field_selection_rule", connection, fieldString, pkString))
        )
        connector <- ZIO.succeed(
          MsSqlReader(
            ConnectionOptions(connectionUrl, "dbo", "field_selection_rule", None),
            new ColumnSummaryFieldsFilteringService(fieldSelectionRule)
          )
        )
        expected <- ZIO.succeed("""declare @currentVersion bigint = CHANGE_TRACKING_CURRENT_VERSION()
              |
              |SELECT
              |tq.[x],
              |CAST(0 as BIGINT) as SYS_CHANGE_VERSION,
              |'I' as SYS_CHANGE_OPERATION,
              |tq.[y],
              |tq.[e],
              |@currentVersion AS 'ChangeTrackingVersion',
              |lower(convert(nvarchar(128), HashBytes('SHA2_256', cast(tq.[x] as nvarchar(128))),2)) as [ARCANE_MERGE_KEY]
              |FROM [arcane].[dbo].[field_selection_rule] tq""".stripMargin)
        query <- QueryProvider.getBackfillQuery(connector)
      yield assertTrue(query == expected)
    },
    test("QueryProvider does not allow PKs in filters") {
      for
        fieldSelectionRule <- ZIO.succeed(new FieldSelectionRuleSettings {
          override val rule: FieldSelectionRule = ExcludeFields(Set("x"))
          override val essentialFields: Set[String] =
            Set("SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION", "ARCANE_MERGE_KEY", "ChangeTrackingVersion")
          override val isServerSide: Boolean = true
        })
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection =>
            ZIO.attemptBlocking(createTable("field_selection_rule_no_pk", connection, fieldString, pkString))
        )
        connector <- ZIO.succeed(
          MsSqlReader(
            ConnectionOptions(connectionUrl, "dbo", "field_selection_rule_no_pk", None),
            new ColumnSummaryFieldsFilteringService(fieldSelectionRule)
          )
        )
        result <- QueryProvider.getBackfillQuery(connector).exit
      yield zio.test.assert(result)(
        fails(
          hasMessage(equalTo("Fields ['x'] are primary keys, and cannot be filtered out by the field selection rule"))
        )
      )
    },
    test("QueryProvider enforces PKs in include filters") {
      for
        fieldSelectionRule <- ZIO.succeed(new FieldSelectionRuleSettings {
          override val rule: FieldSelectionRule = IncludeFields(Set("a", "b", "z"))
          override val essentialFields: Set[String] =
            Set("SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION", "ARCANE_MERGE_KEY", "ChangeTrackingVersion")
          override val isServerSide: Boolean = true
        })
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection => ZIO.attemptBlocking(createTable("field_selection_rule_pk", connection, fieldString, pkString))
        )
        connector <- ZIO.succeed(
          MsSqlReader(
            ConnectionOptions(connectionUrl, "dbo", "field_selection_rule_pk", None),
            new ColumnSummaryFieldsFilteringService(fieldSelectionRule)
          )
        )
        result <- QueryProvider.getBackfillQuery(connector).exit
      yield zio.test.assert(result)(
        fails(hasMessage(equalTo("Fields ['x'] are primary keys, and must be included in the field selection rule")))
      )
    },
    test("MsSqlConnection extracts schema columns from the database") {
      for
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection => ZIO.attemptBlocking(createTable("extracts_schema_columns", connection, fieldString, pkString))
        )
        connector <- ZIO.succeed(
          MsSqlReader(
            ConnectionOptions(connectionUrl, "dbo", "extracts_schema_columns", None),
            emptyFieldsFilteringService
          )
        )
        expected <- ZIO.succeed(
          List(
            Field("x", IntType),
            Field("SYS_CHANGE_VERSION", LongType),
            Field("SYS_CHANGE_OPERATION", StringType),
            Field("y", IntType),
            Field("z", BigDecimalType(30, 6)),
            Field("a", ByteArrayType),
            Field("b", TimestampType),
            Field("cd", IntType),
            Field("e", FloatType),
            Field("ChangeTrackingVersion", LongType),
            MergeKeyField
          )
        )
        schema <- connector.getSchema
      yield zio.test.assert(expected)(equalTo(schema.collect { case f: ArcaneSchemaField =>
        f
      }))
    },
    test("MsSqlConnection returns correct number of rows on backfill") {
      for
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection =>
            ZIO
              .attemptBlocking(createTable("backfill_rows", connection, fieldString, pkString))
              .flatMap(_ => insertData(connection, "backfill_rows"))
        )
        connector <- ZIO.succeed(
          MsSqlReader(
            ConnectionOptions(connectionUrl, "dbo", "backfill_rows", None),
            emptyFieldsFilteringService
          )
        )
        rows <- connector.backfill.runCollect
      yield assertTrue(rows.size == 20)
    },
    test("MsSqlConnection returns correct number of columns on backfill") {
      for
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection =>
            ZIO
              .attemptBlocking(createTable("backfill_columns", connection, fieldString, pkString))
              .flatMap(_ => insertData(connection, "backfill_columns"))
        )
        connector <- ZIO.succeed(
          MsSqlReader(
            ConnectionOptions(connectionUrl, "dbo", "backfill_columns", None),
            emptyFieldsFilteringService
          )
        )
        rows <- connector.backfill.runCollect
      yield assertTrue(rows.head.size == 11)
    },
    test("MsSqlConnection returns correct number of columns on backfill with filter") {
      for
        fieldSelectionRule <- ZIO.succeed(new FieldSelectionRuleSettings {
          override val rule: FieldSelectionRule = IncludeFields(Set("a", "b", "x"))
          override val essentialFields: Set[String] =
            Set("SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION", "ARCANE_MERGE_KEY", "ChangeTrackingVersion")
          override val isServerSide: Boolean = true
        })
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection =>
            ZIO
              .attemptBlocking(createTable("backfill_columns_filtered", connection, fieldString, pkString))
              .flatMap(_ => insertData(connection, "backfill_columns_filtered"))
        )
        connector <- ZIO.succeed(
          MsSqlReader(
            ConnectionOptions(connectionUrl, "dbo", "backfill_columns_filtered", None),
            new ColumnSummaryFieldsFilteringService(fieldSelectionRule)
          )
        )
        expected <- ZIO.succeed(
          List("x", "SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION", "a", "b", "ChangeTrackingVersion", "ARCANE_MERGE_KEY")
        )
        rows <- connector.backfill.runCollect
      yield zio.test.assert(rows.head.map(_.name))(equalTo(expected))
    },
    test("MsSqlConnection returns correct number of rows on getChanges") {
      for
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection =>
            ZIO
              .attemptBlocking(createTable("get_changes_rows", connection, fieldString, pkString))
              .flatMap(_ => insertData(connection, "get_changes_rows"))
        )
        connector <- ZIO.succeed(
          MsSqlReader(
            ConnectionOptions(connectionUrl, "dbo", "get_changes_rows", None),
            emptyFieldsFilteringService
          )
        )
        rows <- connector
          .getChanges(
            MsSqlWatermark(
              version = "1",
              timestamp = OffsetDateTime.ofInstant(Instant.now().minus(Duration.ofDays(1)), ZoneOffset.UTC)
            )
          )
          .runCollect
      yield assertTrue(rows.size == 20)
    },
    test("MsSqlConnection returns correct number of rows on getChanges with filter") {
      for
        fieldSelectionRule <- ZIO.succeed(new FieldSelectionRuleSettings {
          override val rule: FieldSelectionRule = IncludeFields(Set("a", "x"))
          override val essentialFields: Set[String] =
            Set("SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION", "ARCANE_MERGE_KEY", "ChangeTrackingVersion")
          override val isServerSide: Boolean = true
        })
        expected <- ZIO.succeed(
          List(
            "x",
            "SYS_CHANGE_VERSION",
            "SYS_CHANGE_OPERATION",
            "a",
            "ChangeTrackingVersion",
            "ARCANE_MERGE_KEY"
          )
        )
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection =>
            ZIO
              .attemptBlocking(createTable("get_changes_rows_filtered", connection, fieldString, pkString))
              .flatMap(_ => insertData(connection, "get_changes_rows_filtered"))
        )
        connector <- ZIO.succeed(
          MsSqlReader(
            ConnectionOptions(connectionUrl, "dbo", "get_changes_rows_filtered", None),
            new ColumnSummaryFieldsFilteringService(fieldSelectionRule)
          )
        )
        rows <- connector
          .getChanges(
            MsSqlWatermark(
              version = "1",
              timestamp = OffsetDateTime.ofInstant(Instant.now().minus(Duration.ofDays(1)), ZoneOffset.UTC)
            )
          )
          .runCollect
      yield zio.test.assert(rows.head.map(_.name))(equalTo(expected))
    },
    test("MsSqlConnection returns correct number of columns on getChanges") {
      for
        expected <- ZIO.succeed(
          List(
            "x",
            "SYS_CHANGE_VERSION",
            "SYS_CHANGE_OPERATION",
            "y",
            "z",
            "a",
            "b",
            "cd",
            "e",
            "ChangeTrackingVersion",
            "ARCANE_MERGE_KEY"
          )
        )
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection =>
            ZIO
              .attemptBlocking(createTable("get_changes_columns", connection, fieldString, pkString))
              .flatMap(_ => insertData(connection, "get_changes_columns"))
        )
        connector <- ZIO.succeed(
          MsSqlReader(
            ConnectionOptions(connectionUrl, "dbo", "get_changes_columns", None),
            emptyFieldsFilteringService
          )
        )
        rows <- connector
          .getChanges(
            MsSqlWatermark(
              version = "1",
              timestamp = OffsetDateTime.ofInstant(Instant.now().minus(Duration.ofDays(1)), ZoneOffset.UTC)
            )
          )
          .runCollect
      yield zio.test.assert(rows.head.map(_.name))(equalTo(expected))
    },
    test("MsSqlConnection handles deletes") {
      for
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection =>
            ZIO
              .attemptBlocking(createTable("get_changes_deletes", connection, fieldString, pkString))
              .flatMap(_ => insertData(connection, "get_changes_deletes"))
        )
        connector <- ZIO.succeed(
          MsSqlReader(
            ConnectionOptions(connectionUrl, "dbo", "get_changes_deletes", None),
            emptyFieldsFilteringService
          )
        )
        nextTime     <- ZIO.succeed(OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC))
        startTime    <- ZIO.succeed(nextTime.minus(Duration.ofDays(1)))
        maybeVersion <- connector.getVersion(QueryProvider.getVersionFromTimestampQuery(startTime, formatter))
        version      <- ZIO.getOrFail(maybeVersion)
        commitTime   <- connector.getVersionCommitTime(version)
        rows         <- connector.getChanges(MsSqlWatermark.fromChangeTrackingVersion(version, commitTime)).runCollect
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection => deleteData(connection, Seq(2), "get_changes_deletes")
        )

        rowsAfterDelete <- connector
          .getChanges(MsSqlWatermark.fromChangeTrackingVersion(version, nextTime))
          .runCollect
      yield assertTrue(
        rowsAfterDelete.exists(row =>
          row.contains(DataCell("SYS_CHANGE_OPERATION", StringType, "D")) && row.contains(
            DataCell("ARCANE_MERGE_KEY", StringType, "913da1f8df6f8fd47593840d533ba0458cc9873996bf310460abb495b34c232a")
          )
        )
      ) // NOTE: the value here is computed manually
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
