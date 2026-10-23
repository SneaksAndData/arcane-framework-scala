package com.sneaksanddata.arcane.framework
package services.iceberg

import logging.ZIOLogAnnotations.zlog
import models.settings.iceberg.IcebergCatalogSettings
import services.iceberg.base.CatalogFactory

import org.apache.iceberg.CatalogProperties
import org.apache.iceberg.catalog.SessionCatalog.SessionContext
import org.apache.iceberg.rest.auth.OAuth2Properties
import org.apache.iceberg.rest.{HTTPClient, RESTSessionCatalog}
import zio.{Scope, Task, ZIO}

import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions

final class IcebergCatalogFactory(
    icebergCatalogSettings: IcebergCatalogSettings,
    scope: Scope
) extends CatalogFactory:

  def getSessionContext: SessionContext =
    SessionContext(
      java.util.UUID.randomUUID().toString,
      null,
      icebergCatalogSettings.additionalProperties
        .filter(c => c._1 == OAuth2Properties.CREDENTIAL || c._1 == OAuth2Properties.TOKEN)
        .asJava,
      Map().asJava
    );

  def getCatalog: Task[RESTSessionCatalog] =
    scope.extend(IcebergCatalogFactory.newCatalog(icebergCatalogSettings))

object IcebergCatalogFactory:
  private def getCatalogProperties(icebergCatalogSettings: IcebergCatalogSettings): Map[String, String] =
    Map(
      CatalogProperties.WAREHOUSE_LOCATION -> icebergCatalogSettings.warehouse,
      CatalogProperties.URI                -> icebergCatalogSettings.catalogUri,
      "rest-metrics-reporting-enabled"     -> "false",
      "view-endpoints-supported"           -> "false"
    ) ++ icebergCatalogSettings.additionalProperties

  private def newCatalog(icebergCatalogSettings: IcebergCatalogSettings): ZIO[Scope, Throwable, RESTSessionCatalog] =
    ZIO.acquireRelease(for
      properties <- ZIO.succeed(getCatalogProperties(icebergCatalogSettings))
      result <- ZIO.succeed(
        new RESTSessionCatalog(
          config => HTTPClient.builder(config).uri(config.get(CatalogProperties.URI)).build(),
          null
        )
      )
      name <- ZIO.succeed(java.util.UUID.randomUUID().toString)
      _    <- zlog("Creating new Iceberg RESTSessionCatalog instance with id %s", name)
      _ <- ZIO.attemptBlocking(
        result.initialize(name, (properties ++ Map("header.X-Arcane-Runner-Identifier" -> name)).asJava)
      )
    yield result)(catalog =>
      zlog("Closing RESTSessionCatalog %s due to cache expiry or application shutdown", catalog.name()) *> ZIO
        .attemptBlocking(catalog.close())
        .orDieWith(e => new Throwable("Unable to close RESTSessionCatalog instance correctly", e))
    )

  /** Provide an instance of IcebergCatalogFactory
    * @return
    */
  def live(icebergCatalogSettings: IcebergCatalogSettings): ZIO[Scope, Throwable, IcebergCatalogFactory] =
    ZIO.scope.map(new IcebergCatalogFactory(icebergCatalogSettings, _))
