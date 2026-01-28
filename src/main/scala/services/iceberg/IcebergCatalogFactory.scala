package com.sneaksanddata.arcane.framework
package services.iceberg

import logging.ZIOLogAnnotations.zlog
import models.settings.IcebergCatalogSettings
import services.iceberg.base.CatalogFactory

import org.apache.iceberg.CatalogProperties
import org.apache.iceberg.aws.s3.{S3FileIO, S3FileIOProperties}
import org.apache.iceberg.catalog.SessionCatalog.SessionContext
import org.apache.iceberg.rest.auth.OAuth2Properties
import org.apache.iceberg.rest.{HTTPClient, RESTSessionCatalog}
import zio.{Task, ZIO}

import java.time.Instant
import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions

final class IcebergCatalogFactory(icebergCatalogSettings: IcebergCatalogSettings) extends CatalogFactory:
  
  private val catalogProperties: Map[String, String] =
    Map(
      CatalogProperties.WAREHOUSE_LOCATION -> icebergCatalogSettings.warehouse,
      CatalogProperties.URI -> icebergCatalogSettings.catalogUri,
      "rest-metrics-reporting-enabled" -> "false",
      "view-endpoints-supported" -> "false"
    ) ++ icebergCatalogSettings.additionalProperties

  private val maxCatalogLifetime =
    zio.Duration.fromSeconds(10 * 60).toSeconds // limit catalog instance lifetime to 10 minutes
  private val catalogs: TrieMap[String, (RESTSessionCatalog, Long)] = TrieMap()

  def getSessionContext: SessionContext =
    SessionContext(
      java.util.UUID.randomUUID().toString,
      null,
      icebergCatalogSettings.additionalProperties
        .filter(c => c._1 == OAuth2Properties.CREDENTIAL || c._1 == OAuth2Properties.TOKEN)
        .asJava,
      Map().asJava
    );

  def newCatalog: Task[RESTSessionCatalog] = for
    result <- ZIO.succeed(
      new RESTSessionCatalog(
        config => HTTPClient.builder(config).uri(config.get(CatalogProperties.URI)).build(),
        (_, _) =>
          val baseIO = new S3FileIO()
          baseIO.initialize(catalogProperties.asJava)
          baseIO
      )
    )
    name <- ZIO.succeed(java.util.UUID.randomUUID().toString)
    _ <- zlog("Creating new Iceberg RESTSessionCatalog instance with id %s", name)
    _ <- ZIO.attemptBlocking(
      result.initialize(name, (catalogProperties ++ Map("header.X-Arcane-Runner-Identifier" -> name)).asJava)
    )
    _ <- ZIO.attempt(catalogs.addOne((name, (result, Instant.now.getEpochSecond))))
  yield result

  def getCatalog: Task[RESTSessionCatalog] = for
    catalogInfo <- ZIO.attempt(catalogs.find { case (_, (_, duration)) =>
      Instant.now.getEpochSecond - duration < maxCatalogLifetime
    })
    selected <- catalogInfo match {
      case Some((_, (catalog, _))) => ZIO.succeed(catalog)
      case None =>
        ZIO
          .attempt(catalogs.foreach { case (_, (catalog, _)) =>
            catalog.close()
          })
          .map(_ => catalogs.clear())
          .flatMap(_ => newCatalog)
    }
  yield selected
