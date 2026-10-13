package com.sneaksanddata.arcane.framework
package tests.catalog

import models.settings.iceberg.IcebergCatalogSettings
import services.iceberg.{IcebergCatalogFactory, IcebergSinkTablePropertyManager}

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import zio.test.*
import zio.test.TestAspect.{sequential, timeout, withLiveClock}
import zio.{Duration, Scope, ZIO}

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

/** Reproduces a race between RESTSessionCatalog rotation (Cached.auto) and in-flight catalog usage: the previous
  * catalog is closed while callers may still hold or use it, which surfaces as "Unable to load target table ...".
  *
  * Uses a local stub of the Iceberg REST catalog, so no external services are required.
  */
object IcebergCatalogRotationTests extends ZIOSpecDefault:
  private val tableName = "rotation_test_table"

  private val configResponse = """{"defaults":{},"overrides":{}}"""

  private val tableResponse =
    """{
      |  "metadata-location": "s3://warehouse/rotation_test_table/metadata/00000.metadata.json",
      |  "metadata": {
      |    "format-version": 2,
      |    "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c0",
      |    "location": "s3://warehouse/rotation_test_table",
      |    "last-sequence-number": 0,
      |    "last-updated-ms": 1700000000000,
      |    "last-column-id": 1,
      |    "current-schema-id": 0,
      |    "schemas": [{"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "a", "required": false, "type": "string"}]}],
      |    "default-spec-id": 0,
      |    "partition-specs": [{"spec-id": 0, "fields": []}],
      |    "last-partition-id": 999,
      |    "default-sort-order-id": 0,
      |    "sort-orders": [{"order-id": 0, "fields": []}],
      |    "properties": {},
      |    "current-snapshot-id": -1,
      |    "snapshots": []
      |  }
      |}""".stripMargin

  private def respond(exchange: HttpExchange, body: String): Unit =
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.add("Content-Type", "application/json")
    exchange.sendResponseHeaders(200, bytes.length)
    exchange.getResponseBody.write(bytes)
    exchange.close()

  private def stubCatalog(loadTableDelayMs: Long): ZIO[Scope, Throwable, HttpServer] =
    ZIO.acquireRelease(ZIO.attemptBlocking {
      val server = HttpServer.create(new InetSocketAddress("localhost", 0), 0)
      server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool())
      server.createContext(
        "/",
        (exchange: HttpExchange) =>
          if exchange.getRequestURI.getPath.endsWith("/v1/config") then respond(exchange, configResponse)
          else
            Thread.sleep(loadTableDelayMs)
            respond(exchange, tableResponse)
      )
      server.start()
      server
    })(server => ZIO.succeed(server.stop(0)))

  private def settings(server: HttpServer, lifetime: Duration): IcebergCatalogSettings = new IcebergCatalogSettings:
    override val namespace: String                         = "test"
    override val warehouse: String                         = "demo"
    override val catalogUri: String                        = s"http://localhost:${server.getAddress.getPort}"
    override val additionalProperties: Map[String, String] = Map.empty
    override val maxCatalogInstanceLifetime: Duration      = lifetime

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("IcebergCatalogFactory rotation")(
    test("a catalog obtained before rotation remains usable by its in-flight caller") {
      for
        server  <- stubCatalog(loadTableDelayMs = 0)
        factory <- IcebergCatalogFactory.live(settings(server, Duration.fromMillis(500)))
        catalog <- factory.getCatalog
        // wait for at least one rotation, which closes the catalog instance obtained above
        _ <- ZIO.sleep(Duration.fromMillis(1500))
        result <- ZIO
          .attemptBlocking(
            catalog.loadTable(factory.getSessionContext, org.apache.iceberg.catalog.TableIdentifier.of("test", tableName))
          )
          .either
      yield assertTrue(result.isRight)
    },
    test("property manager does not fail when catalog rotates during a slow loadTable request") {
      for
        server  <- stubCatalog(loadTableDelayMs = 1500)
        cfg = settings(server, Duration.fromMillis(500))
        factory <- IcebergCatalogFactory.live(cfg)
        manager = IcebergSinkTablePropertyManager(cfg, factory)
        result <- manager.getTableSchema(tableName).either
      yield assertTrue(result.isRight)
    }
  ) @@ sequential @@ withLiveClock @@ timeout(Duration.fromSeconds(60))
