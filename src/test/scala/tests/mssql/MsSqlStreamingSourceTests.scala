package com.sneaksanddata.arcane.framework
package tests.mssql

import models.schemas.ArcaneType.*
import models.schemas.{ArcaneSchemaField, DataCell, IndexedField, IndexedMergeKeyField, MergeKeyField}
import models.settings.*
import models.settings.mssql.MsSqlServerDatabaseSourceSettings
import models.settings.sources.{SurrogateMergeKey, SurrogateMergeKeyImpl}
import services.mssql.QueryProvider
import services.mssql.QueryProvider.getBackfillQuery
import services.mssql.base.{ColumnSummary, ColumnSummaryFieldSelector, MsSqlStreamingSource}
import services.mssql.query.ResultSetIterator
import services.mssql.versioning.MsSqlWatermark
import services.naming.DefaultNameGenerator
import tests.mssql.util.MsSqlTestServices
import tests.mssql.util.MsSqlTestServices.*
import tests.shared.TestSinkSettings
import utils.HashUtils

import org.scalatest.*
import org.scalatest.matchers.should.Matchers.*
import zio.stream.ZStream
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

object MsSqlStreamingSourceTests extends ZIOSpecDefault:
  private implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  private val fieldString = "(x int not null, y int, z DECIMAL(30, 6), a VARBINARY(MAX), b DATETIME, [c/d] int, e real)"
  private val pkString    = "primary key(x)"
  private val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  private val nopSelector: ColumnSummaryFieldSelector = new ColumnSummaryFieldSelector(new FieldSelectionRuleSettings {

    /** The field selection rule to use.
      */
    override val rule: FieldSelectionRule = AllFieldsImpl(AllFields())

    /** The set of essential fields that must ALWAYS be included in the field selection rule. Fields from this list are
      * used in SQL queries and ALWAYS must be present in the result set. This list is provided by the Arcane streaming
      * plugin and should not be configurable.
      */
    override val essentialFields: Set[String] = Set.empty[String]
    override val isServerSide: Boolean        = true
  })

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

  private val nameGenerator =
    new DefaultNameGenerator(
      sinkSettings = TestSinkSettings,
      backfillId = "",
      streamId = "mssql_reader_tests"
    )

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("MsSqlStreamingSourceTests")(
    test("QueryProvider generates columns query") {
      for
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection => ZIO.attemptBlocking(createTable("columns_query_test", connection, fieldString, pkString))
        )
        reader <- ZIO.succeed(
          MsSqlStreamingSource(
            new MsSqlServerDatabaseSourceSettings {
              override val connectionUrl: String                          = MsSqlTestServices.connectionUrl
              override val schemaName: String                             = "dbo"
              override val tableName: String                              = "columns_query_test"
              override val fetchSize: Option[Int]                         = None
              override val extraConnectionParameters: Map[String, String] = Map.empty
              override val shardSizeMegabytes: Option[Int]                = None
              override val backfillShardSchemaName: String                = "dbo"
            },
            nopSelector,
            nameGenerator,
            Seq.empty
          )
        )
        query <- QueryProvider.getColumnSummariesQuery(
          reader.connectionSettings.schemaName,
          reader.connectionSettings.tableName,
          reader.catalog
        )
      yield assertTrue(query.contains("case when kcu.CONSTRAINT_NAME is not null then 1 else 0 end as IsPrimaryKey"))
    },
    test("QueryProvider generates schema query") {
      for
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection => ZIO.attemptBlocking(createTable("schema_query_test", connection, fieldString, pkString))
        )
        reader <- ZIO.succeed(
          MsSqlStreamingSource(
            new MsSqlServerDatabaseSourceSettings {
              override val connectionUrl: String                          = MsSqlTestServices.connectionUrl
              override val schemaName: String                             = "dbo"
              override val tableName: String                              = "schema_query_test"
              override val fetchSize: Option[Int]                         = None
              override val extraConnectionParameters: Map[String, String] = Map.empty
              override val shardSizeMegabytes: Option[Int]                = None
              override val backfillShardSchemaName: String                = "dbo"
            },
            nopSelector,
            nameGenerator,
            Seq.empty
          )
        )
        query <- QueryProvider.getSchemaQuery(reader)
      yield assertTrue(query.contains("ct.SYS_CHANGE_VERSION"))
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
        reader <- ZIO.succeed(
          MsSqlStreamingSource(
            new MsSqlServerDatabaseSourceSettings {
              override val connectionUrl: String                          = MsSqlTestServices.connectionUrl
              override val schemaName: String                             = "dbo"
              override val tableName: String                              = "backfill_query"
              override val fetchSize: Option[Int]                         = None
              override val extraConnectionParameters: Map[String, String] = Map.empty
              override val shardSizeMegabytes: Option[Int]                = None
              override val backfillShardSchemaName: String                = "dbo"
            },
            nopSelector,
            nameGenerator,
            Seq.empty
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
            |@currentVersion AS 'ChangeTrackingVersion'
            |FROM [arcane].[dbo].[backfill_query] tq""".stripMargin)
        summaries <- reader.getColumnSummaries
        query     <- reader.getBackfillQuery("dbo", "backfill_query", summaries)
      yield assertTrue(query == expected)
    },
    test("QueryProvider handles field selection rule") {
      for
        fieldSelectionRule <- ZIO.succeed(new FieldSelectionRuleSettings {
          override val rule: FieldSelectionRule = ExcludeFieldsImpl(ExcludeFields(Set("b", "a", "z", "cd")))
          override val essentialFields: Set[String] =
            Set("SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION", "ChangeTrackingVersion")
          override val isServerSide: Boolean = true
        })
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection => ZIO.attemptBlocking(createTable("field_selection_rule", connection, fieldString, pkString))
        )
        reader <- ZIO.succeed(
          MsSqlStreamingSource(
            new MsSqlServerDatabaseSourceSettings {
              override val connectionUrl: String                          = MsSqlTestServices.connectionUrl
              override val schemaName: String                             = "dbo"
              override val tableName: String                              = "field_selection_rule"
              override val fetchSize: Option[Int]                         = None
              override val extraConnectionParameters: Map[String, String] = Map.empty
              override val shardSizeMegabytes: Option[Int]                = None
              override val backfillShardSchemaName: String                = "dbo"
            },
            new ColumnSummaryFieldSelector(fieldSelectionRule),
            nameGenerator,
            Seq.empty
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
              |@currentVersion AS 'ChangeTrackingVersion'
              |FROM [arcane].[dbo].[field_selection_rule] tq""".stripMargin)
        summaries <- reader.getColumnSummaries
        query     <- reader.getBackfillQuery("dbo", "field_selection_rule", summaries)
      yield assertTrue(query == expected)
    },
    test("QueryProvider does not allow PKs in filters") {
      for
        fieldSelectionRule <- ZIO.succeed(new FieldSelectionRuleSettings {
          override val rule: FieldSelectionRule = ExcludeFieldsImpl(ExcludeFields(Set("x")))
          override val essentialFields: Set[String] =
            Set("SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION", "ChangeTrackingVersion")
          override val isServerSide: Boolean = true
        })
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection =>
            ZIO.attemptBlocking(createTable("field_selection_rule_no_pk", connection, fieldString, pkString))
        )
        reader <- ZIO.succeed(
          MsSqlStreamingSource(
            new MsSqlServerDatabaseSourceSettings {
              override val connectionUrl: String                          = MsSqlTestServices.connectionUrl
              override val schemaName: String                             = "dbo"
              override val tableName: String                              = "field_selection_rule_no_pk"
              override val fetchSize: Option[Int]                         = None
              override val extraConnectionParameters: Map[String, String] = Map.empty
              override val shardSizeMegabytes: Option[Int]                = None
              override val backfillShardSchemaName: String                = "dbo"
            },
            new ColumnSummaryFieldSelector(fieldSelectionRule),
            nameGenerator,
            Seq.empty
          )
        )

        tryGetSummaries <- reader.getColumnSummaries.exit
      yield zio.test.assert(tryGetSummaries)(
        fails(
          hasMessage(equalTo("Fields ['x'] are primary keys, and cannot be filtered out by the field selection rule"))
        )
      )
    },
    test("QueryProvider enforces PKs in include filters") {
      for
        fieldSelectionRule <- ZIO.succeed(new FieldSelectionRuleSettings {
          override val rule: FieldSelectionRule = IncludeFieldsImpl(IncludeFields(Set("a", "b", "z")))
          override val essentialFields: Set[String] =
            Set("SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION", "ChangeTrackingVersion")
          override val isServerSide: Boolean = true
        })
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection => ZIO.attemptBlocking(createTable("field_selection_rule_pk", connection, fieldString, pkString))
        )
        reader <- ZIO.succeed(
          MsSqlStreamingSource(
            new MsSqlServerDatabaseSourceSettings {
              override val connectionUrl: String                          = MsSqlTestServices.connectionUrl
              override val schemaName: String                             = "dbo"
              override val tableName: String                              = "field_selection_rule_pk"
              override val fetchSize: Option[Int]                         = None
              override val extraConnectionParameters: Map[String, String] = Map.empty
              override val shardSizeMegabytes: Option[Int]                = None
              override val backfillShardSchemaName: String                = "dbo"
            },
            new ColumnSummaryFieldSelector(fieldSelectionRule),
            nameGenerator,
            Seq.empty
          )
        )

        tryGetSummaries <- reader.getColumnSummaries.exit
      yield zio.test.assert(tryGetSummaries)(
        fails(hasMessage(equalTo("Fields ['x'] are primary keys, and must be included in the field selection rule")))
      )
    },
    test("MsSqlStreamingSource extracts schema columns from the database") {
      for
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection => ZIO.attemptBlocking(createTable("extracts_schema_columns", connection, fieldString, pkString))
        )
        reader <- ZIO.succeed(
          MsSqlStreamingSource(
            new MsSqlServerDatabaseSourceSettings {
              override val connectionUrl: String                          = MsSqlTestServices.connectionUrl
              override val schemaName: String                             = "dbo"
              override val tableName: String                              = "extracts_schema_columns"
              override val fetchSize: Option[Int]                         = None
              override val extraConnectionParameters: Map[String, String] = Map.empty
              override val shardSizeMegabytes: Option[Int]                = None
              override val backfillShardSchemaName: String                = "dbo"
            },
            nopSelector,
            nameGenerator,
            Seq.empty
          )
        )
        expected <- ZIO.succeed(
          List(
            IndexedField("x", IntType, 0),
            IndexedField("SYS_CHANGE_VERSION", LongType, 1),
            IndexedField("SYS_CHANGE_OPERATION", StringType, 2),
            IndexedField("y", IntType, 3),
            IndexedField("z", BigDecimalType(30, 6), 4),
            IndexedField("a", ByteArrayType, 5),
            IndexedField("b", TimestampType, 6),
            IndexedField("cd", IntType, 7),
            IndexedField("e", FloatType, 8),
            IndexedField("ChangeTrackingVersion", LongType, 9)
          )
        )
        schema <- reader.getSchema
      yield zio.test.assert(expected)(equalTo(schema.collect { case f: ArcaneSchemaField =>
        f
      }))
    },
    test("MsSqlStreamingSource returns correct number of rows on a shard stream") {
      for
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection =>
            ZIO
              .attemptBlocking(createTable("backfill_rows", connection, fieldString, pkString))
              .flatMap(_ => insertData(connection, "backfill_rows"))
        )
        reader <- ZIO.succeed(
          MsSqlStreamingSource(
            new MsSqlServerDatabaseSourceSettings {
              override val connectionUrl: String                          = MsSqlTestServices.connectionUrl
              override val schemaName: String                             = "dbo"
              override val tableName: String                              = "backfill_rows"
              override val fetchSize: Option[Int]                         = None
              override val extraConnectionParameters: Map[String, String] = Map.empty
              override val shardSizeMegabytes: Option[Int]                = None
              override val backfillShardSchemaName: String                = "dbo"
            },
            nopSelector,
            nameGenerator,
            Seq.empty
          )
        )

        summaries <- reader.getColumnSummaries
        rows      <- ZStream.fromZIO(reader.createShardStream("backfill_rows", summaries)).flatMap(_._1).runCollect
      yield assertTrue(rows.size == 20)
    },
    test("MsSqlStreamingSource injects surrogate merge keys for composite string primary keys") {
      val testTableName    = "surrogate_merge_key"
      val primaryKeyValue1 = "string-key-1"
      val primaryKeyValue2 = "string-key-2"

      for
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie) {
          connection =>
            ZIO.attemptBlocking {
              createTable(
                testTableName,
                connection,
                "(id1 varchar(128) not null, id2 varchar(128) not null, value int)",
                "primary key(id1, id2)"
              )
              val statement = connection.createStatement()
              try
                statement.executeUpdate(
                  s"use arcane; insert into dbo.$testTableName values('$primaryKeyValue1', '$primaryKeyValue2', 1)"
                )
              finally statement.close()
            }
        }
        reader <- ZIO.succeed(
          MsSqlStreamingSource(
            new MsSqlServerDatabaseSourceSettings {
              override val connectionUrl: String                          = MsSqlTestServices.connectionUrl
              override val schemaName: String                             = "dbo"
              override val tableName: String                              = testTableName
              override val fetchSize: Option[Int]                         = None
              override val extraConnectionParameters: Map[String, String] = Map.empty
              override val shardSizeMegabytes: Option[Int]                = None
              override val backfillShardSchemaName: String                = "dbo"
            },
            nopSelector,
            nameGenerator,
            Seq(SurrogateMergeKeyImpl(SurrogateMergeKey()))
          )
        )
        schema    <- reader.getSchema
        summaries <- reader.getColumnSummaries
        rows <- ZStream
          .fromZIO(reader.createShardStream(testTableName, summaries))
          .flatMap(_._1)
          .runCollect
        mergeKey         = rows.head.find(_.name == MergeKeyField.name).map(_.value)
        expectedMergeKey = HashUtils.murmur3(s"$primaryKeyValue1#$primaryKeyValue2")
      yield assertTrue(
        schema.exists {
          case _: IndexedMergeKeyField => true
          case _                       => false
        },
        mergeKey.contains(expectedMergeKey)
      )
    },
    test("MsSqlStreamingSource returns correct number of columns on a shard stream") {
      for
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection =>
            ZIO
              .attemptBlocking(createTable("backfill_columns", connection, fieldString, pkString))
              .flatMap(_ => insertData(connection, "backfill_columns"))
        )
        reader <- ZIO.succeed(
          MsSqlStreamingSource(
            new MsSqlServerDatabaseSourceSettings {
              override val connectionUrl: String                          = MsSqlTestServices.connectionUrl
              override val schemaName: String                             = "dbo"
              override val tableName: String                              = "backfill_columns"
              override val fetchSize: Option[Int]                         = None
              override val extraConnectionParameters: Map[String, String] = Map.empty
              override val shardSizeMegabytes: Option[Int]                = None
              override val backfillShardSchemaName: String                = "dbo"
            },
            nopSelector,
            nameGenerator,
            Seq.empty
          )
        )

        summaries <- reader.getColumnSummaries
        rows      <- ZStream.fromZIO(reader.createShardStream("backfill_columns", summaries)).flatMap(_._1).runCollect
      yield assertTrue(rows.head.size == 10)
    },
    test("MsSqlStreamingSource returns correct number of columns on a shard stream with filter") {
      for
        fieldSelectionRule <- ZIO.succeed(new FieldSelectionRuleSettings {
          override val rule: FieldSelectionRule = IncludeFieldsImpl(IncludeFields(Set("a", "b", "x")))
          override val essentialFields: Set[String] =
            Set("SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION", "ChangeTrackingVersion")
          override val isServerSide: Boolean = true
        })
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection =>
            ZIO
              .attemptBlocking(createTable("backfill_columns_filtered", connection, fieldString, pkString))
              .flatMap(_ => insertData(connection, "backfill_columns_filtered"))
        )
        reader <- ZIO.succeed(
          MsSqlStreamingSource(
            new MsSqlServerDatabaseSourceSettings {
              override val connectionUrl: String                          = MsSqlTestServices.connectionUrl
              override val schemaName: String                             = "dbo"
              override val tableName: String                              = "backfill_columns_filtered"
              override val fetchSize: Option[Int]                         = None
              override val extraConnectionParameters: Map[String, String] = Map.empty
              override val shardSizeMegabytes: Option[Int]                = None
              override val backfillShardSchemaName: String                = "dbo"
            },
            new ColumnSummaryFieldSelector(fieldSelectionRule),
            nameGenerator,
            Seq.empty
          )
        )
        expected <- ZIO.succeed(
          List("x", "SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION", "a", "b", "ChangeTrackingVersion")
        )

        summaries <- reader.getColumnSummaries
        rows <- ZStream
          .fromZIO(reader.createShardStream("backfill_columns_filtered", summaries))
          .flatMap(_._1)
          .runCollect
      yield zio.test.assert(rows.head.map(_.name))(equalTo(expected))
    },
    test("MsSqlStreamingSource returns correct number of rows on getChanges") {
      for
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection =>
            ZIO
              .attemptBlocking(createTable("get_changes_rows", connection, fieldString, pkString))
              .flatMap(_ => insertData(connection, "get_changes_rows"))
        )
        reader <- ZIO.succeed(
          MsSqlStreamingSource(
            new MsSqlServerDatabaseSourceSettings {
              override val connectionUrl: String                          = MsSqlTestServices.connectionUrl
              override val schemaName: String                             = "dbo"
              override val tableName: String                              = "get_changes_rows"
              override val fetchSize: Option[Int]                         = None
              override val extraConnectionParameters: Map[String, String] = Map.empty
              override val shardSizeMegabytes: Option[Int]                = None
              override val backfillShardSchemaName: String                = "dbo"
            },
            nopSelector,
            nameGenerator,
            Seq.empty
          )
        )
        rows <- reader
          .getChanges(
            MsSqlWatermark(
              version = "1",
              timestamp = OffsetDateTime.ofInstant(Instant.now().minus(Duration.ofDays(1)), ZoneOffset.UTC)
            )
          )
          .flatMap(_._1)
          .runCollect
      yield assertTrue(rows.size == 20)
    },
    test("MsSqlStreamingSource returns correct number of rows on getChanges with filter") {
      for
        fieldSelectionRule <- ZIO.succeed(new FieldSelectionRuleSettings {
          override val rule: FieldSelectionRule = IncludeFieldsImpl(IncludeFields(Set("a", "x")))
          override val essentialFields: Set[String] =
            Set("SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION", "ChangeTrackingVersion")
          override val isServerSide: Boolean = true
        })
        expected <- ZIO.succeed(
          List(
            "x",
            "SYS_CHANGE_VERSION",
            "SYS_CHANGE_OPERATION",
            "a",
            "ChangeTrackingVersion"
          )
        )
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection =>
            ZIO
              .attemptBlocking(createTable("get_changes_rows_filtered", connection, fieldString, pkString))
              .flatMap(_ => insertData(connection, "get_changes_rows_filtered"))
        )
        reader <- ZIO.succeed(
          MsSqlStreamingSource(
            new MsSqlServerDatabaseSourceSettings {
              override val connectionUrl: String                          = MsSqlTestServices.connectionUrl
              override val schemaName: String                             = "dbo"
              override val tableName: String                              = "get_changes_rows_filtered"
              override val fetchSize: Option[Int]                         = None
              override val extraConnectionParameters: Map[String, String] = Map.empty
              override val shardSizeMegabytes: Option[Int]                = None
              override val backfillShardSchemaName: String                = "dbo"
            },
            new ColumnSummaryFieldSelector(fieldSelectionRule),
            nameGenerator,
            Seq.empty
          )
        )
        rows <- reader
          .getChanges(
            MsSqlWatermark(
              version = "1",
              timestamp = OffsetDateTime.ofInstant(Instant.now().minus(Duration.ofDays(1)), ZoneOffset.UTC)
            )
          )
          .flatMap(_._1)
          .runCollect
      yield zio.test.assert(rows.head.map(_.name))(equalTo(expected))
    },
    test("MsSqlStreamingSource returns correct number of columns on getChanges") {
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
            "ChangeTrackingVersion"
          )
        )
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection =>
            ZIO
              .attemptBlocking(createTable("get_changes_columns", connection, fieldString, pkString))
              .flatMap(_ => insertData(connection, "get_changes_columns"))
        )
        reader <- ZIO.succeed(
          MsSqlStreamingSource(
            new MsSqlServerDatabaseSourceSettings {
              override val connectionUrl: String                          = MsSqlTestServices.connectionUrl
              override val schemaName: String                             = "dbo"
              override val tableName: String                              = "get_changes_columns"
              override val fetchSize: Option[Int]                         = None
              override val extraConnectionParameters: Map[String, String] = Map.empty
              override val shardSizeMegabytes: Option[Int]                = None
              override val backfillShardSchemaName: String                = "dbo"
            },
            nopSelector,
            nameGenerator,
            Seq.empty
          )
        )
        rows <- reader
          .getChanges(
            MsSqlWatermark(
              version = "1",
              timestamp = OffsetDateTime.ofInstant(Instant.now().minus(Duration.ofDays(1)), ZoneOffset.UTC)
            )
          )
          .flatMap(_._1)
          .runCollect
      yield zio.test.assert(rows.head.map(_.name))(equalTo(expected))
    },
    test("MsSqlStreamingSource handles deletes") {
      for
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection =>
            ZIO
              .attemptBlocking(createTable("get_changes_deletes", connection, fieldString, pkString))
              .flatMap(_ => insertData(connection, "get_changes_deletes"))
        )
        reader <- ZIO.succeed(
          MsSqlStreamingSource(
            new MsSqlServerDatabaseSourceSettings {
              override val connectionUrl: String                          = MsSqlTestServices.connectionUrl
              override val schemaName: String                             = "dbo"
              override val tableName: String                              = "get_changes_deletes"
              override val fetchSize: Option[Int]                         = None
              override val extraConnectionParameters: Map[String, String] = Map.empty
              override val shardSizeMegabytes: Option[Int]                = None
              override val backfillShardSchemaName: String                = "dbo"
            },
            nopSelector,
            nameGenerator,
            Seq.empty
          )
        )
        nextTime     <- ZIO.succeed(OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC))
        startTime    <- ZIO.succeed(nextTime.minus(Duration.ofDays(1)))
        maybeVersion <- reader.getVersion(QueryProvider.getVersionFromTimestampQuery(startTime, formatter))
        version      <- ZIO.getOrFail(maybeVersion)
        commitTime   <- reader.getVersionCommitTime(version)
        rows         <- reader.getChanges(MsSqlWatermark.fromChangeTrackingVersion(version, commitTime)).runCollect
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection => deleteData(connection, Seq(2), "get_changes_deletes")
        )

        rowsAfterDelete <- reader
          .getChanges(MsSqlWatermark.fromChangeTrackingVersion(version, nextTime))
          .flatMap(_._1)
          .runCollect
      yield assertTrue(
        rowsAfterDelete.exists(row => row.contains(DataCell("SYS_CHANGE_OPERATION", StringType, "D")))
      )
    },
    test("MsSqlStreamingSource deleteShards correctly filters matching tables minimizing LIKE wildcards impact") {
      for
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie) {
          connection =>
            ZIO.attemptBlocking {
              val st = connection.createStatement()
              st.execute(
                "use arcane; drop table if exists dbo.[backfill__s1__table1]; create table dbo.[backfill__s1__table1] (x int)"
              )
              st.execute(
                "use arcane; drop table if exists dbo.[backfill__s1__table2]; create table dbo.[backfill__s1__table2] (x int)"
              )
              st.execute(
                "use arcane; drop table if exists dbo.[backfill__s11__table1]; create table dbo.[backfill__s11__table1] (x int)"
              )
              st.execute(
                "use arcane; drop table if exists dbo.[backfill__s11__table2]; create table dbo.[backfill__s11__table2] (x int)"
              )
              st.close()
            }
        }
        reader <- ZIO.succeed(
          MsSqlStreamingSource(
            new MsSqlServerDatabaseSourceSettings {
              override val connectionUrl: String                          = MsSqlTestServices.connectionUrl
              override val schemaName: String                             = "dbo"
              override val tableName: String                              = "backfill__s1__table1"
              override val fetchSize: Option[Int]                         = None
              override val extraConnectionParameters: Map[String, String] = Map.empty
              override val shardSizeMegabytes: Option[Int]                = None
              override val backfillShardSchemaName: String                = "dbo"
            },
            nopSelector,
            nameGenerator,
            Seq.empty
          )
        )
        _ <- reader.deleteShards("backfill__s1__")
        remainingTables <- ZIO.acquireReleaseWith(getConnection)(connection =>
          ZIO.attemptBlocking(connection.close()).orDie
        ) { connection =>
          ZIO
            .attemptBlocking {
              val st = connection.createStatement()
              new ResultSetIterator(
                st.executeQuery(
                  QueryProvider.getFindMatchingTablesQuery("backfill__s1", "dbo")
                )
              )
            }
            .flatMap(iter =>
              ZIO.foldLeft(iter.to(Iterable))(List[String]())((agg, v) => ZIO.succeed(agg :+ v.head.value.toString))
            )
        }
      yield assertTrue(remainingTables == List("backfill__s11__table1", "backfill__s11__table2"))
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
