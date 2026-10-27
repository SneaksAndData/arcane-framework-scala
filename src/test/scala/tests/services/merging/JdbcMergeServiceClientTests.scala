package com.sneaksanddata.arcane.framework
package tests.services.merging

import models.batches.SynapseLinkMergeBatch
import models.schemas.ArcaneType.{BooleanType, StringType}
import models.schemas.{ArcaneSchema, Field, MergeKeyField, VersionField}
import services.base.SchemaProvider
import services.merging.*
import services.metrics.DeclaredMetrics
import tests.services.merging.JdbcMergeServiceClientTests.test
import tests.shared.{TestJdbcMergeServiceClientSettings, TestTablePropertiesSettings}

import io.trino.jdbc.TrinoDriver
import org.scalatestplus.easymock.EasyMockSugar.mock
import zio.test.TestAspect.timeout
import zio.test.{Spec, TestAspect, TestEnvironment, ZIOSpecDefault, assertTrue}
import zio.{Scope, Task, ZIO}

import java.sql.Connection
import java.util.{Properties, UUID}

object JdbcMergeServiceClientTests extends ZIOSpecDefault:
  private val connectionUri = "jdbc:trino://localhost:8080/iceberg/test?user=test"
  private val schema = MergeKeyField :: VersionField :: Field("IsDelete", BooleanType) :: Field(
    "colA",
    StringType
  ) :: Field("colB", StringType) :: Field("Id", StringType) :: Nil

  private val schemaProviderMock = mock[SchemaProvider[ArcaneSchema]]

  private def getJdbcMergeServiceClient =
    new JdbcMergeServiceClient(
      TestJdbcMergeServiceClientSettings,
      "iceberg",
      "test",
      DeclaredMetrics(),
      false
    )

  private def getConnection: Task[Connection] =
    for
      driver     <- ZIO.succeed(new TrinoDriver())
      connection <- ZIO.attemptBlocking(driver.connect(connectionUri, new Properties()))
    yield connection

  private def createTable(tableName: String, connection: Connection): Task[Unit] =
    for
      statement <- ZIO.attemptBlocking(connection.createStatement())

      dropTableStatement = s"DROP TABLE IF EXISTS iceberg.test.$tableName"
      _ <- ZIO.attemptBlocking(statement.execute(dropTableStatement))
      createTableStatement =
        s"CREATE TABLE IF NOT EXISTS iceberg.test.$tableName (ARCANE_MERGE_KEY VARCHAR, ${VersionField.name} BIGINT, IsDelete BOOLEAN, colA VARCHAR, colB VARCHAR, Id VARCHAR)"
      _ <- ZIO.attemptBlocking(statement.execute(createTableStatement))
    yield ()

  private def insertValues(tableName: String, connection: Connection): Task[Unit] =
    for
      updateStatement <- ZIO.attemptBlocking(connection.createStatement())
      _ <- ZIO.foreach(1 to 10) { i =>
        val insertCmd =
          s"insert into iceberg.test.$tableName (ARCANE_MERGE_KEY, ${VersionField.name}, IsDelete, colA, colB, Id) values ('$i', $i, false, '$i', '$i', '$i')"
        ZIO.attemptBlocking(updateStatement.execute(insertCmd))
      }
    yield ()

  private def setupTable(tableName: String): Task[Unit] =
    for _ <- ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie) {
        connection =>
          for
            _ <- createTable(tableName, connection)
            _ <- createTable(s"staged_$tableName", connection)
            _ <- insertValues(s"staged_$tableName", connection)
          yield ()
      }
    yield ()

  /** A purely CPU-bound query (no joins/large allocations) that is slow enough (~5-10s) to give a reliable window to
    * interrupt it mid-flight, while staying safe on memory. The leading comment embeds a unique marker so the query
    * can be correlated with its Trino `query_id` via `system.runtime.queries`.
    */
  private def slowQuery(marker: String): String =
    s"""-- $marker
       |SELECT count(*) FROM (
       |  SELECT a.x,
       |    reduce(sequence(1,10000), 0,
       |      (s,i) -> s + reduce(sequence(1,500), 0, (s2,j) -> s2 + (i*j % 104729), s2 -> s2),
       |      s -> s) r
       |  FROM UNNEST(sequence(1,2000)) a(x)
       |)
       |WHERE r > 0""".stripMargin

  /** Looks up the Trino `query_id` of the running query submitted with the given marker comment.
    *
    * `JdbcMergeServiceClient` always goes through `Connection.prepareStatement`, which the Trino JDBC driver
    * executes as two separate server-side queries: a `PREPARE statement1 FROM <sql>` (which carries our marker
    * comment) followed by an `EXECUTE statement1` (which does not). This correlates the two by finding the `EXECUTE`
    * query immediately following the matching `PREPARE`, using a dedicated connection so the lookup is not blocked
    * by the (possibly still running) statement being observed.
    */
  private def findQueryId(marker: String): Task[Option[String]] =
    ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie) { connection =>
      for
        statement <- ZIO.attemptBlocking(connection.createStatement())
        rs <- ZIO.attemptBlocking(
          statement.executeQuery(
            s"""WITH p AS (
               |  SELECT created FROM system.runtime.queries
               |  WHERE query LIKE 'PREPARE statement1 FROM -- $marker%'
               |  ORDER BY created DESC LIMIT 1
               |)
               |SELECT q.query_id FROM system.runtime.queries q, p
               |WHERE q.query = 'EXECUTE statement1' AND q.created >= p.created
               |ORDER BY q.created ASC LIMIT 1""".stripMargin
          )
        )
        result <- ZIO.attemptBlocking(if rs.next() then Some(rs.getString(1)) else None)
      yield result
    }

  /** Reads the current state (and error type, if any) of a Trino query by its `query_id`.
    */
  private def getQueryState(queryId: String): Task[Option[(String, Option[String])]] =
    ZIO.acquireReleaseWith(getConnection)(connection => ZIO.attemptBlocking(connection.close()).orDie) { connection =>
      for
        statement <- ZIO.attemptBlocking(connection.createStatement())
        rs <- ZIO.attemptBlocking(
          statement.executeQuery(s"SELECT state, error_type FROM system.runtime.queries WHERE query_id = '$queryId'")
        )
        result <- ZIO.attemptBlocking(
          if rs.next() then Some((rs.getString(1), Option(rs.getString(2)))) else None
        )
      yield result
    }

  private val nonTerminalStates = Set("QUEUED", "PLANNING", "STARTING", "RUNNING")

  /** Repeatedly evaluates `poll` until `isDone` returns true for its result, waiting `delay` between attempts, for at
    * most `maxAttempts` attempts. Returns the last observed result once `isDone` succeeds or attempts are exhausted.
    */
  private def pollUntil[Result](maxAttempts: Int, delay: zio.Duration)(poll: Task[Result])(
      isDone: Result => Boolean
  ): Task[Result] =
    poll.flatMap { result =>
      if isDone(result) || maxAttempts <= 1 then ZIO.succeed(result)
      else ZIO.sleep(delay) *> pollUntil(maxAttempts - 1, delay)(poll)(isDone)
    }

  private def pollUntilTerminal(queryId: String): Task[Option[(String, Option[String])]] =
    pollUntil(maxAttempts = 20, delay = zio.Duration.fromMillis(500))(getQueryState(queryId)) {
      case Some((state, _)) => !nonTerminalStates.contains(state)
      case None              => false
    }

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("JdbcMergeServiceClientTests")(
    test("applies a batch to target table") {
      for
        tableName <- ZIO.succeed("table_a")
        batch <- ZIO.succeed(
          SynapseLinkMergeBatch(
            s"test.staged_$tableName",
            schema,
            s"test.$tableName",
            TestTablePropertiesSettings
          )
        )
        _ <- setupTable(tableName)

        connection <- getConnection
        mergeServiceClient = getJdbcMergeServiceClient
        _  <- mergeServiceClient.applyBatch(batch)
        rs <- ZIO.attemptBlocking(connection.createStatement().executeQuery(s"SELECT count(1) FROM ${batch.name}"))
        _  <- ZIO.attemptBlocking(rs.next())
      yield assertTrue(rs.getInt(1) == 10)
    },
    test("cancels an in-flight query on the server when the executing fiber is interrupted") {
      for
        marker             <- ZIO.succeed(s"jdbcq_${UUID.randomUUID().toString.replace("-", "")}")
        mergeServiceClient <- ZIO.succeed(getJdbcMergeServiceClient)

        fiber <- mergeServiceClient.execute(slowQuery(marker), "slow_query", "Testing").fork

        // Wait until the query is visible to Trino and still running, simulating that a pod shutdown would
        // interrupt the fiber while the merge query is genuinely in flight.
        queryIdOpt <- pollUntil(maxAttempts = 15, delay = zio.Duration.fromMillis(200))(findQueryId(marker))(
          _.isDefined
        )
        queryId = queryIdOpt.getOrElse(throw new RuntimeException(s"Query with marker $marker was never observed"))

        _ <- fiber.interruptFork

        // Bounded by both a max poll count and a hard wall-clock budget (10s)
        finalState <- pollUntilTerminal(queryId)
      yield assertTrue(
        finalState.exists { case (state, _) => state == "FAILED" }
      )
    }
  ) @@ TestAspect.sequential @@ timeout(zio.Duration.fromSeconds(30)) @@ TestAspect.withLiveClock
