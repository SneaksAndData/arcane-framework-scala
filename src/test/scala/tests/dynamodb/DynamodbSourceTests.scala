package com.sneaksanddata.arcane.framework
package tests.dynamodb

import tests.shared.TestSinkSettings

import services.storage.models.dynamodb.DynamodbClientSettings

import models.schemas.ArcaneType.*
import models.schemas.{ArcaneSchemaField, DataCell, IndexedField, IndexedMergeKeyField}
import models.settings.*
import services.naming.DefaultNameGenerator

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

object DynamodbSourceTests extends ZIOSpecDefault:
  private implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  private val fieldString = "(x int not null, y int, z DECIMAL(30, 6), a VARBINARY(MAX), b DATETIME, [c/d] int, e real)"
  private val pkString    = "primary key(x)"
  private val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  private val emptyFieldsFilteringService: MsSqlServerFieldsFilteringService = (fields: List[ColumnSummary]) =>
    Success(fields)

  def insertData(con: Connection, tableName: String): Task[Unit] = ???

  def updateData(con: Connection, tableName: String): Task[Unit] = ???

  def deleteData(connection: Connection, primaryKeys: Seq[Int], tableName: String): ZIO[Any, Throwable, Unit] = ???

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("DynamodbConnectionTests")(
    test("DynamoDB has changes") {
      for
        _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie)(
          connection => ZIO.attemptBlocking(createTable("columns_query_test", connection, fieldString, pkString))
        )
        reader <- ZIO.succeed(
          DynamodbStreamingSource(
            new DynamodbClientSettings {
              override val connectionUrl: String = "localhost:4566"
              override val tableName: String     = "test"
            }
          )
        )
        query <- QueryProvider.getColumnSummariesQuery(
          reader.connectionSettings.schemaName,
          reader.connectionSettings.tableName,
          reader.catalog
        )
      yield assertTrue(query.contains("case when kcu.CONSTRAINT_NAME is not null then 1 else 0 end as IsPrimaryKey"))
    }
  ) @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
{}
